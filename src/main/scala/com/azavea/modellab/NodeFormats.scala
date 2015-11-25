package com.azavea.modellab

import com.azavea.modellab.op._
import spray.json._
import spray.json.DefaultJsonProtocol._
import geotrellis.raster.op.local
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
          json.get[String]("name") match {
            case "LoadLayer" =>
              LoadLayerFormat.read(json)
            case "FocalAspect" =>
              FocalAspectFormat.read(json)
            case "FocalSlope" =>
              FocalSlopeFormat.read(json)
            case "MapValues" =>
              MapValuesFormat.read(json)
            case "MapRanges" =>
              MapRangesFormat.read(json)
            case "ConvertToFloat" =>
              CoerceFormat.read(json)
            case name if name.startsWith("LocalUnary") =>
              LocalUnaryFormat.read(json)
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
      case op: LocalUnary =>
        LocalUnaryFormat.write(op)
      case op: Focal =>
        FocalFormat.write(op)
      case op: FocalSlope =>
        FocalSlopeFormat.write(op)
      case op: FocalAspect =>
        FocalAspectFormat.write(op)
      case op: MapValues =>
        MapValuesFormat.write(op)
      case op: MapRanges =>
        MapRangesFormat.write(op)
      case op: Coerce =>
        CoerceFormat.write(op)
    }
  }

  // Helper function to convert nodes to JsObject
  private def writeNode(node: Op, name: String, paramFields: (String, JsValue)*) =
    JsObject(Map.empty
      + ("name" -> JsString(name))
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

  implicit object CoerceFormat extends JsonFormat[Coerce] {
    def read(json: JsValue) = {
      Coerce(json.inputs.head)
    }

    def write(o: Coerce) =
      writeNode(o, "ConvertToFloat", "ConvertToFloat" -> true.toJson)
  }

  implicit object MapRangesFormat extends JsonFormat[MapRanges] {
    def read(json: JsValue) = {
      val mappings = json.param[Seq[((Double, Double), Option[Double])]]("mappings")
      MapRanges(json.inputs.head, mappings)
    }

    def write(o: MapRanges) =
      writeNode(o, s"MapRanges", "mappings" -> o.mappings.toJson)
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
      require(json.inputs.size == 1, "FocalAspect expect one layer input")
      FocalAspect(json.inputs.head, json.param[Neighborhood]("neighborhood"))
    }

    def write(o: FocalAspect) =
      writeNode(o, "FocalAspect", "neighborhood" -> o.n.toJson)
  }

  implicit object FocalSlopeFormat extends JsonFormat[FocalSlope] {
    def read(json: JsValue) = {
      require(json.inputs.size == 1, "FocalAspect expect one layer input")
      FocalSlope(json.inputs.head, json.param[Neighborhood]("neighborhood"), json.param[Double]("z_factor"))
    }

    def write(o: FocalSlope) =
      writeNode(o, "FocalSlope", "neighborhood" -> o.n.toJson, "z_factor" -> o.z.toJson)
  }

  implicit object LocalBinaryFormat extends JsonFormat[LocalBinary] {
    def read(json: JsValue) = {
      val inputs = json.inputs
      val constant = json.optionalParam[Double]("constant")
      
      json.get[String]("name") match {
        case "LocalAdd" => 
          LocalBinary(local.Add, inputs, constant)
        case "LocalSubtract" => 
          LocalBinary(local.Subtract, inputs, constant)
        case "LocalMultiply" =>
          LocalBinary(local.Multiply, inputs, constant)
        case "LocalDivide" =>
          LocalBinary(local.Divide, inputs, constant)
        case "LocalMin" =>
          LocalBinary(local.Min, inputs, constant)
        case "LocalPow" =>
          LocalBinary(local.Pow, inputs, constant)
        case "LocalAtan2" =>
          LocalBinary(Atan2, inputs, constant)
      }
    }

    def write(o: LocalBinary) =
      writeNode(o, s"Local${o.op.name}", "constant" -> o.const.toJson)
  }

  implicit object LocalUnaryFormat extends JsonFormat[LocalUnary] {
    def read(json: JsValue) = {
      val inputs = json.inputs
      
      json.get[String]("name") match {
        case "LocalUnarySqrt" =>
          LocalUnary("LocalUnarySqrt", local.Sqrt.apply, inputs.head)
        case "LocalUnaryRound" =>
          LocalUnary("LocalUnaryRound", local.Round.apply, inputs.head)
        case "LocalUnaryLog" =>
          LocalUnary("LocalUnaryLog", local.Log.apply, inputs.head)
        case "LocalUnaryLog10" =>
          LocalUnary("LocalUnaryLog10", local.Log10.apply, inputs.head)
        case "LocalUnaryFloor" =>
          LocalUnary("LocalUnaryFloor", local.Floor.apply, inputs.head)
        case "LocalUnaryCeil" =>
          LocalUnary("LocalUnaryCeil", local.Ceil.apply, inputs.head)
        case "LocalUnaryNegate" =>
          LocalUnary("LocalUnaryNegate", local.Negate.apply, inputs.head)
        case "LocalUnaryNot" =>
          LocalUnary("LocalUnaryNot", local.Not.apply, inputs.head)
        case "LocalUnaryAbs" =>
          LocalUnary("LocalUnaryAbs", local.Abs.apply, inputs.head)
        case "LocalUnaryAcos" =>
          LocalUnary("LocalUnaryAcos", local.Acos.apply, inputs.head)
        case "LocalUnaryAsin" =>
          LocalUnary("LocalUnaryAsin", local.Asin.apply, inputs.head)
        case "LocalUnaryAtan" =>
          LocalUnary("LocalUnaryAtan", local.Atan.apply, inputs.head)
        case "LocalUnaryCos" =>
          LocalUnary("LocalUnaryCos", local.Cos.apply, inputs.head)
        case "LocalUnarySin" =>
          LocalUnary("LocalUnarySin", local.Sin.apply, inputs.head)
        case "LocalUnaryTan" =>
          LocalUnary("LocalUnaryTan", local.Tan.apply, inputs.head)
        case "LocalUnaryCosh" =>
          LocalUnary("LocalUnaryCosh", local.Cosh.apply, inputs.head)
        case "LocalUnaryTanh" =>
          LocalUnary("LocalUnaryTanh", local.Tanh.apply, inputs.head)
        case "LocalUnarySinh" =>
          LocalUnary("LocalUnarySinh", local.Sinh.apply, inputs.head)
      }
    }

    def write(o: LocalUnary) =
      writeNode(o, s"Local${o.name}", "constant" -> 0.toJson)
  }

  implicit object FocalFormat extends JsonFormat[Focal] {
    def read(json: JsValue) = {
      require(json.inputs.size == 1, "Focal operations expect one layer input")
      val input = json.inputs.head
      val n = json.param[Neighborhood]("neighborhood")

      json.get[String]("name") match {
        case "FocalSum" =>
          Focal("Sum", focal.Sum.apply, input, n)
        case "FocalMax" =>          
          Focal("Max", focal.Max.apply, input, n)
        case "FocalMin" =>
          Focal("Min", focal.Min.apply, input, n)
        case "FocalMean" =>
          Focal("Mean", focal.Mean.apply, input, n)
        case "FocalMedian" =>
          Focal("Median", focal.Median.apply, input, n)
        case "FocalMode" =>
          Focal("Mode", focal.Mode.apply, input, n)
        case "FocalStandardDeviation" =>
          Focal("StandardDeviation", focal.StandardDeviation.apply, input, n)
        case "FocalConway" =>
          Focal("Conway", focal.Conway.apply, input, n)
      }
    }

    def write(o: Focal) =
      writeNode(o, s"Focal${o.name}", "neighborhood" -> o.n.toJson)
  }
}
