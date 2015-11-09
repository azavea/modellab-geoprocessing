package com.azavea.modellab

import com.azavea.modellab.op._
import spray.json._
import spray.json.DefaultJsonProtocol._
import geotrellis.raster.op.local._
import geotrellis.raster.op.focal
import geotrellis.raster.op.focal._

/**
 * Transforms JSON Tree to Node object tree
 * @param windowedReader  used by LoadLayer, which is the leaf in Node object tree
 */
class NodeFormats(windowedReader: WindowedReader, layerLookup: String => Option[Op]) {

  // Node trait doesn't have enough information to implement this, so this is mostly a type switch
  implicit object OpFormat extends JsonFormat[Op] {
    def read(json: JsValue) = {
      json match {
        case JsString(hash) =>
          layerLookup(hash).getOrElse(
            throw new DeserializationException(s"Failed to find layer for hash: $hash"))
        case JsObject(fields) =>
          json.get[String]("function_name") match {
            case "LoadLayer" =>
              LoadLayerFormat.read(json)
            case "FocalAspect" =>
              FocalAspectFormat.read(json)
            case "FocalSlope" =>
              SlopeOpFormat.read(json)
            case "MapValues" =>
              MapValuesFormat.read(json)
            case name if name.startsWith("Local") =>
              LocalBinaryFormat.read(json)
            case name if name.startsWith("Focal") =>
              FocalFormat.read(json)
          }
        case _ =>
          throw new DeserializationException(s"Node definition may either be an object or hash string")
      }
    }

    def write(node: Op): JsValue = node match {
      case op: LoadLayer =>
        LoadLayerFormat.write(op)
      case op: LocalBinary =>
        LocalBinaryFormat.write(op)
      case op: Focal =>
        FocalFormat.write(op)
      case op: FocalSlope =>
        SlopeOpFormat.write(op)
      case op: FocalAspect =>
        FocalAspectFormat.write(op)
      case op: MapValues =>
        MapValuesFormat.write(op)
    }
  }

  // Helper function to convert nodes to JsObject
  private def writeNode(node: Op, name: String, paramFields: (String, JsValue)*) =
    JsObject(Map.empty
      + ("function_name" -> JsString(name))
      + ("hash" -> JsString(node.hashString))
      ++ paramFields
      + ("inputs" -> node.inputs.toJson)
    )

  //lets make it easy on ourselves to work with JsValue that is actually a Node
  implicit class withJsonMethods(json: JsValue) {
    val fields = json.asJsObject.fields
    def get[T: JsonReader](name: String) = fields(name).convertTo[T]
    def inputs: Seq[Op] = fields("inputs").convertTo[Seq[Op]]
    def param[T: JsonReader](name: String): T = {
      fields(name).convertTo[T]
    }
    def optionalParam[T: JsonFormat](name: String): Option[T] = {
      for {
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

  implicit object MapValuesFormat extends JsonFormat[MapValues] {
    def read(json: JsValue) = {
      val mappings = json.param[Seq[(Seq[Int], Option[Int])]]("mappings")
      MapValues(json.inputs.head, mappings)
    }

    def write(o: MapValues) =
      writeNode(o, s"MapValues", "mappings" -> o.mappings.toJson)
  }

  implicit object LoadLayerFormat extends JsonFormat[LoadLayer] {
    def read(json: JsValue) = {
      LoadLayer(json.param[String]("layer_name"), windowedReader)
    }

    def write(o: LoadLayer) = 
      writeNode(o, s"LoadLayer", "layer_name" -> JsString(o.layerName)) 
  }

  implicit object FocalAspectFormat extends JsonFormat[FocalAspect] {
    def read(json: JsValue) = {
      require(json.inputs.size == 1, "FocalAspect expexect one layer input")
      FocalAspect(json.inputs.head, json.param[Neighborhood]("neighborhood"))
    }

    def write(o: FocalAspect) =
      writeNode(o, "FocalAspect", "neighborhood" -> o.n.toJson)
  }

  implicit object SlopeOpFormat extends JsonFormat[FocalSlope] {
    def read(json: JsValue) = {
      require(json.inputs.size == 1, "FocalAspect expexect one layer input")
      FocalSlope(json.inputs.head, json.param[Neighborhood]("neighborhood"), json.param[Double]("z_factor"))
    }

    def write(o: FocalSlope) =
      writeNode(o, "FocalSlope", "neighborhood" -> o.n.toJson, "z_factor" -> o.z.toJson)
  }

  implicit object LocalBinaryFormat extends JsonFormat[LocalBinary] {
    def read(json: JsValue) = {
      val inputs = json.inputs
      val constant = json.optionalParam[Double]("constant")
      
      json.get[String]("function_name") match {
        case "LocalAdd" => 
          LocalBinary(Add, inputs, constant)
        case "LocalSubtract" => 
          LocalBinary(Subtract, inputs, constant)
        case "LocalMultiply" =>
          LocalBinary(Multiply, inputs, constant)
        case "LocalDivide" =>
          LocalBinary(Divide, inputs, constant)
      }
    }

    def write(o: LocalBinary) =
      writeNode(o, s"Local${o.op.name}", "constant" -> o.const.toJson)
  }

  implicit object FocalFormat extends JsonFormat[Focal] {
    def read(json: JsValue) = {
      require(json.inputs.size == 1, "Focal operations expexect one layer input")
      val input = json.inputs.head
      val n = json.param[Neighborhood]("neighborhood")

      json.get[String]("function_name") match {
        case "FocalSum" =>
          Focal("Sum", focal.Sum.apply, input, n)
        case "FocalMax" =>          
          Focal("Max", focal.Max.apply, input, n)
        case "FocalMin" =>
          Focal("Min", focal.Min.apply, input, n)
        case "FocalMean" =>
          Focal("Mean", focal.Mean.apply, input, n)
      }
    }

    def write(o: Focal) =
      writeNode(o, s"Focal${o.name}", "neighborhood" -> o.n.toJson)
  }
}
