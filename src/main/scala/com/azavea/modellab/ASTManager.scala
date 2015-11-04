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


class ASTManager(parser: Parser) {

  def transform(json: JsValue): Node = parser.nodeReader.read(json)

  def sync(json: JsValue, nodeCB: Node => Unit, jsonCB: JsValue => Unit): Unit = {
    // Do Work
    val node = transform(json)
    val updatedJson = (json.convertTo[JsObject].fields + Tuple2("guid", JsNumber(node.id))).toJson
    nodeCB(node)
    jsonCB(updatedJson)

    // Recurse
    node.inputs match {
      case Nil => ()
      case inputs@_ => {
        for {
          nInput <- inputs
          jInput <- json.convertTo[JsObject].fields("inputs").convertTo[JsArray].elements
        } sync(jInput, nodeCB, jsonCB)
      }
    }
  }
}

object ASTManager {
  def apply(parser: Parser): ASTManager = new ASTManager(parser)
}

