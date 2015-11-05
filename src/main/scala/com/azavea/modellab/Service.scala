package com.azavea.modellab

import akka.actor.ActorSystem

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.histogram._
import geotrellis.raster.io.json._
import geotrellis.raster.render._
import geotrellis.raster.resample._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.avro.codecs._
import geotrellis.spark.io.json._
import geotrellis.spark.io.s3._
import geotrellis.spark.tiling._
import geotrellis.spark.utils.SparkUtils
import geotrellis.vector._
import geotrellis.vector.reproject._

import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global

import spray.http._
import spray.httpx.encoding._
import spray.httpx.unmarshalling._
import spray.httpx.marshalling._
import spray.httpx.SprayJsonSupport._
import spray.json._
import spray.routing._
import MediaTypes._
import spray.http.HttpHeaders.RawHeader

object Service extends SimpleRoutingApp with DataHubCatalog with Instrumented with App {
  private[this] lazy val requestTimer = metrics.timer("tileRequest")

  implicit val system = ActorSystem("spray-system")
  implicit val sc = geotrellis.spark.utils.SparkUtils.createLocalSparkContext("local[*]", "Model Service")

  val colorBreaks = mutable.HashMap.empty[String, ColorBreaks]

  val registry = new LayerRegistry(layerReader)

  val pingPong = path("ping")(complete("pong"))

  def registerLayerRoute = 
    post {
      requestInstance { req =>
        respondWithHeader(RawHeader("Access-Control-Allow-Origin", "*")) {
          complete {
            import DefaultJsonProtocol._
            val json = req.entity.asString.parseJson      
            registry.register(json)
          }
        }
      } 
    } ~ 
    get {
      pathPrefix(Segment) { layerHash =>
        complete{
          import DefaultJsonProtocol._
          registry.getLayerJson(layerHash) 
        }
      }
    }

  def registerColorBreaksRoute =
    pathPrefix(Segment) { breaksName =>
      post {
        requestInstance { req =>
          complete {
            import java.math.BigInteger

            val blob = req.entity.asString
            val breaks = {
              val split = blob.split(";").map(_.trim.split(":"))
              println(split.toList)
              val limits = split.map(pair => Integer.parseInt(pair(0)))
              val colors = split.map(pair => new BigInteger(pair(1), 16).intValue())
              ColorBreaks(limits, colors)
            }

            colorBreaks.update(breaksName, breaks)
            println(s"Registered Breaks: $breaksName")
            StatusCodes.OK
          }
        }
      }
    }


  def guidRoute = pathPrefix(Segment / IntNumber / IntNumber / IntNumber) { (guid, zoom, x, y) =>
    parameters('breaks.?) { breaksName =>
      respondWithMediaType(MediaTypes.`image/png`) {
        complete{ 
          def render(tile: Tile): Array[Byte] = {
            for {
              name <- breaksName
              breaks <- colorBreaks.get(name)
            } yield tile.renderPng(breaks).bytes
          }.getOrElse(tile.renderPng().bytes)

          for { optionFutureTile <- registry.getTile(guid, zoom, x, y) } yield
            for { optionTile <- optionFutureTile }  yield 
              for (tile <- optionTile) yield render(tile)                               
          }
        } 
      }
    }
  

  startServer(interface = "0.0.0.0", port = 8888) {
    pingPong ~ guidRoute ~ pathPrefix("layers"){registerLayerRoute} ~ pathPrefix("breaks"){registerColorBreaksRoute}
  }
}
