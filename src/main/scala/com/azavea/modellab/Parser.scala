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
import org.apache.spark.storage._
import scala.collection.mutable


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
    def param[T: JsonReader](name: String) = 
      fields("parameters")
        .asJsObject
        .fields(name)
        .convertTo[T]

    def inputs: Seq[Node] = fields("inputs").convertTo[Seq[Node]]
  }

  private def readNode: PartialFunction[String, JsValue => Node] = {
    case "LoadLayer" => json =>
      LoadLayer(json.param[String]("layer_name"), windowedReader)

    case "LocalAdd" => json => {
      val inputs = json.inputs
      LocalBinaryOp(Add, Seq(inputs(0), inputs(1)))
    }

    case "LocalSubtract" => json => {
      val inputs = json.inputs
      LocalBinaryOp(Subtract, Seq(inputs(0), inputs(1)))
    }

    case "LocalMultiply" => json => {
      val inputs = json.inputs
      LocalBinaryOp(Multiply, Seq(inputs(0), inputs(1)))
    }

    case "LocalDivide" => json => {
      val inputs = json.inputs
      LocalBinaryOp(Divide, Seq(inputs(0), inputs(1)))
    }

    case "ValueMask" => json => {
      ValueMask(json.inputs.head, json.param[Seq[Int]]("masks"))
    }

    case "FocalSum" => json => {
      val inputs = json.inputs
      val n = Square(1)
      require(inputs.size == 1, "FocalSum expexects one layer input")

      FocalOp(inputs.head, n, Sum.apply)
    }

  }

  def parse(json: JsValue): Node = nodeReader.read(json)
}
