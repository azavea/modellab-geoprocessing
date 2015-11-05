package com.azavea.modellab

import spray.json._
import spray.json.DefaultJsonProtocol._
import shapeless._
import shapeless.ops.hlist._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.raster._
import geotrellis.raster.op.local._
import geotrellis.raster.op.focal
import geotrellis.raster.op.focal._
import geotrellis.raster.op.elevation._
import org.apache.spark.storage._
import scala.collection.mutable
import scala.util.Try


/**
 * Transforms JSON Tree to Node object tree
 * @param WindowedReader  used by LoadLayer, which is the leaf in Node object tree
 */
class NodeFormats(windowedReader: WindowedReader, layerLookup: String => Option[Node]) {

  // Node trait doesn't have enough information to implement this, so this is mostly a type switch
  implicit object NodeFormat extends JsonFormat[Node] {
    def read(json: JsValue) = {
      json match {
        case JsString(hash) =>
          layerLookup(hash).getOrElse(
            throw new DeserializationException(s"Failed to find layer for hash: $hash as specified in $json"))
        case JsObject(fields) =>
          json.get[String]("function_name") match {
            case name if name.startsWith("Local") =>
              LocalBinaryOpFormat.read(json)
            case name if name.startsWith("Focal") =>
              FocalOpFormat.read(json)
            case "Aspect" =>
              AspectOpFormat.read(json)
            case "Slope" => 
              SlopeOpFormat.read(json)
            case "MapValues" =>
              MapValuesOpFormat.read(json)
            case "LoadLayer" =>
              LoadLayerFormat.read(json)
          }
      }
    }

    def write(node: Node): JsValue = node match {
      case op: LoadLayer =>
        LoadLayerFormat.write(op)
      case op: LocalBinaryOp =>
        LocalBinaryOpFormat.write(op)
      case op: FocalOp =>
        FocalOpFormat.write(op)
      case op: SlopeOp =>
        SlopeOpFormat.write(op)
      case op: AspectOp =>
        AspectOpFormat.write(op)
      case op: MapValuesOp =>
        MapValuesOpFormat.write(op)
    }
  }

  // Helper function to convert nodes to JsObject
  private def writeNode(node: Node, name: String, paramFields: (String, JsValue)*) = JsObject(
    "function_name" -> JsString(name),
    "inputs" -> node.inputs.toJson,
    "hash" -> JsString(node.hashString),
    "parameters" -> JsObject(paramFields.toMap))

  //lets make it easy on ourselves to work with JsValue that is actually a Node
  implicit class withJsonMethods(json: JsValue) {
    val fields = json.asJsObject.fields
    def get[T: JsonReader](name: String) = fields(name).convertTo[T]
    def inputs: Seq[Node] = fields("inputs").convertTo[Seq[Node]]
    def param[T: JsonReader](name: String): T = {
      fields("parameters")
        .asJsObject
        .fields(name)
        .convertTo[T]
    }
    def optionalParam[T: JsonFormat](name: String): Option[T] = {
      for {
        paramList <- fields.get("parameters")
        paramObj <- fields.get(name)
        value <- paramObj.convertTo[Option[T]]
      } yield value
    }
  }


  implicit object NeighborhoodFormat extends JsonFormat[Neighborhood] {
    def read(json: JsValue): Neighborhood = {
      val fields = json.asJsObject.fields
      def size[T: JsonReader] = fields("size").convertTo[T]
      fields("shape").convertTo[String] match {
        case "square" => Square(size[Int])
        case "circle" => Circle(size[Int])
        case "nesw" => Nesw(size[Int])
        case "wedge" => {
          val nDims = size[Seq[Int]]
          require(nDims.size == 3, "Wedges require exactly 3 parameters")
          Wedge(nDims(0), nDims(1), nDims(2))
        }
        case "annulus" => {
          val nDims = size[Seq[Int]]
          require(nDims.size == 3, "Annulae require exactly 2 parameters")
          Annulus(nDims(0), nDims(1))
        }
      }
    }

    def write(neighborhood: Neighborhood): JsValue = neighborhood match {
      case shape: Square => {
        val size = shape.extent.toJson
        JsObject("size" -> size, "shape" -> JsString("square"))
      }
      case shape: Circle => {
        val size = shape.extent.toJson
        JsObject("size" -> size, "shape" -> JsString("circle"))
      }
      case shape: Nesw => {
        val size = shape.extent.toJson
        JsObject("size" -> size, "shape" -> JsString("nesw"))
      }
      case shape: Wedge => {
        val size = JsArray(
          shape.radius.toJson,
          shape.startAngle.toJson,
          shape.endAngle.toJson
        )
        JsObject("size" -> size, "shape" -> JsString("wedge"))
      }
      case shape: Annulus => {
        val size = JsArray(
          shape.innerRadius.toJson,
          shape.outerRadius.toJson
        )
        JsObject("size" -> size, "shape" -> JsString("annulus"))
      }
    }
  }

  implicit object MapValuesOpFormat extends JsonFormat[MapValuesOp] {
    def read(json: JsValue) = {
      val mappings = json.param[Seq[(Seq[Int], Option[Int])]]("mappings")
      MapValuesOp(json.inputs.head, mappings)
    }

    def write(o: MapValuesOp) = 
      writeNode(o, s"MapValues", "mappings" -> o.mappings.toJson)
  }

  implicit object LoadLayerFormat extends JsonFormat[LoadLayer] {
    def read(json: JsValue) = {
      LoadLayer(json.param[String]("layer_name"), windowedReader)
    }

    def write(o: LoadLayer) = 
      writeNode(o, s"LoadLayer", "layer_name" -> JsString(o.layerName)) 
  }

  implicit object AspectOpFormat extends JsonFormat[AspectOp] {
    def read(json: JsValue) = {
      require(json.inputs.size == 1, "FocalAspect expexect one layer input")
      AspectOp(json.inputs.head, json.param[Neighborhood]("neighborhood"))
    }

    def write(o: AspectOp) = 
      writeNode(o, "FocalAspect", "neighborhood" -> o.n.toJson)
  }

  implicit object SlopeOpFormat extends JsonFormat[SlopeOp] {
    def read(json: JsValue) = {
      require(json.inputs.size == 1, "FocalAspect expexect one layer input")
      SlopeOp(json.inputs.head, json.param[Neighborhood]("neighborhood"), json.param[Double]("z_factor"))
    }

    def write(o: SlopeOp) = 
      writeNode(o, "FocalSlope", "neighborhood" -> o.n.toJson, "z_factor" -> o.z.toJson)
  }

  implicit object LocalBinaryOpFormat extends JsonFormat[LocalBinaryOp] {
    def read(json: JsValue) = {
      val inputs = json.inputs
      val constant = json.optionalParam[Double]("constant")
      
      json.get[String]("function_name") match {
        case "LocalAdd" => 
          LocalBinaryOp(Add, inputs, constant)
        case "LocalSubtract" => 
          LocalBinaryOp(Subtract, inputs, constant)
        case "LocalMultiply" =>
          LocalBinaryOp(Multiply, inputs, constant)
        case "LocalDivide" =>
          LocalBinaryOp(Divide, inputs, constant)
      }
    }

    def write(o: LocalBinaryOp) = 
      writeNode(o, s"Local${o.op.name}", "constant" -> o.const.toJson)
  }

  implicit object FocalOpFormat extends JsonFormat[FocalOp] {
    def read(json: JsValue) = {
      require(json.inputs.size == 1, "Focal operations expexect one layer input")
      val input = json.inputs.head
      val n = json.param[Neighborhood]("neighborhood")

      json.get[String]("function_name") match {
        case "FocalSum" =>
          FocalOp("Sum", focal.Sum.apply, input, n)
        case "FocalMax" =>          
          FocalOp("Max", focal.Max.apply, input, n)
        case "FocalMin" =>
          FocalOp("Min", focal.Min.apply, input, n)
        case "FocalMean" =>
          FocalOp("Mean", focal.Mean.apply, input, n)
      }
    }

    def write(o: FocalOp) = 
      writeNode(o, s"Focal${o.name}", "neighborhood" -> o.n.toJson)
  }
}
