package com.azavea.modellab

import spray.json._
import spray.json.DefaultJsonProtocol._
import shapeless._
import shapeless.ops.hlist._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.raster._
import geotrellis.raster.op.local._
import geotrellis.raster.op.focal._
import geotrellis.raster.op.elevation._
import org.apache.spark.storage._
import scala.collection.mutable
import scala.util.Try


/**
 * Transforms JSON Tree to Node object tree
 * @param layerRegistry    used to register layers as they are parsed
 * @param layerReader      used by LoadLayer, which is the leaf in Node object tree
 */
class Parser(layerRegistry: LayerRegistry, layerReader: FilteringLayerReader[LayerId, SpatialKey, RasterRDD[SpatialKey]]) {

  val windowedReader = new WindowedReader(layerReader, 8)

  // define this is a JsonFormat so it can be picked up implicitly
  implicit def nodeReader: JsonFormat[Node] = new JsonFormat[Node] {
    def read(json: JsValue): Node = {
      val obj = json.asJsObject
      val name = obj.fields("function_name").convertTo[String]
      val guid = obj.fields("guid").convertTo[String]
      layerRegistry.getLayer(guid) match {
        case Some(node) =>
          node
        case None =>
          // Register all the guids so they may be rendered
          println(json)
          layerRegistry.registerLayer(guid, readNode(name)(json))
      }
    }

    //this is required to make use of DefaultJsonProtocols
    def write(node: Node): JsValue = ???
  }

  //lets make it easy on ourselves to work with JsValue that is actually a Node
  implicit class withJsonMethods(json: JsValue) {
    val fields = json.asJsObject.fields
    def get[T: JsonReader](name: String) = fields(name).convertTo[T]
    def inputs: Seq[Node] = fields("inputs").convertTo[Seq[Node]]
    def param[T: JsonReader](name: String): T =
      fields("parameters")
        .asJsObject
        .fields(name)
        .convertTo[T]
  }


  implicit def  neighborhoodParser: JsonFormat[Neighborhood] = new JsonFormat[Neighborhood] {
    def read(json: JsValue): Neighborhood = {
      val obj = json.asJsObject
      def size[T: JsonReader] = obj.fields("size").convertTo[T]
      obj.fields("shape").convertTo[String] match {
        case "square" => Square(size[Int])
        case "circle" => Circle(size[Int])
        case "nesw" => Nesw(size[Int])
        case "wedge" => {
          val nDims = size[Seq[Int]]
          require(nDims.size == 3, "Wedges require exactly 3 parameters")
          Wedge(nDims(0), nDims(1), nDims(2))
        }
        case "ring" => {
          val nDims = size[Seq[Int]]
          require(nDims.size == 3, "Rings require exactly 2 parameters")
          Annulus(nDims(0), nDims(1))
        }
      }
    }
    //this is required to make use of DefaultJsonProtocols
    def write(neighborhood: Neighborhood): JsValue = ???
  }

  private def readNode: PartialFunction[String, JsValue => Node] = {
    case "LoadLayer" => json =>
      LoadLayer(json.param[String]("layer_name"), windowedReader)

    case "LocalAdd" => json => {
      LocalBinaryOp(Add, json.inputs, json.param[Option[Double]]("constant"))
    }

    case "LocalSubtract" => json => {
      LocalBinaryOp(Subtract, json.inputs, json.param[Option[Double]]("constant"))
    }

    case "LocalMultiply" => json => {
      LocalBinaryOp(Multiply, json.inputs, json.param[Option[Double]]("constant"))
    }

    case "LocalDivide" => json => {
      LocalBinaryOp(Divide, json.inputs, json.param[Option[Double]]("constant"))
    }

    case "Mapping" => json => {
      val mappings = json.param[Seq[(Seq[Int], Option[Int])]]("mappings")
      MapValuesOp(json.inputs.head, mappings)
    }

    case "FocalSum" => json => {
      val n = json.param[Neighborhood]("neighborhood")
      require(json.inputs.size == 1, "FocalSum expexects one layer input")

      FocalOp(Sum.apply, json.inputs.head, n)
    }

    case "FocalMax" => json => {
      val n = json.param[Neighborhood]("neighborhood")
      require(json.inputs.size == 1, "FocalMax expexects one layer input")

      FocalOp(geotrellis.raster.op.focal.Max.apply, json.inputs.head, n)
    }

    case "FocalMin" => json => {
      val n = json.param[Neighborhood]("neighborhood")
      require(json.inputs.size == 1, "FocalMin expexects one layer input")

      FocalOp(geotrellis.raster.op.focal.Min.apply, json.inputs.head, n)
    }

    case "FocalMean" => json => {
      val n = json.param[Neighborhood]("neighborhood")
      require(json.inputs.size == 1, "FocalMin expexects one layer input")

      FocalOp(geotrellis.raster.op.focal.Mean.apply, json.inputs.head, n)
    }

    case "FocalAspect" => json => {
      val n = json.param[Neighborhood]("neighborhood")
      require(json.inputs.size == 1, "FocalMin expexects one layer input")

      AspectOp(Aspect.apply, json.inputs.head, n)
    }

    case "FocalSlope" => json => {
      val n = json.param[Neighborhood]("neighborhood")
      val z = json.param[Double]("z_factor")
      require(json.inputs.size == 1, "FocalMin expexects one layer input")

      SlopeOp(Slope.apply, json.inputs.head, n, z)
    }

  }

  def parse(json: JsValue): Node = nodeReader.read(json)
}
