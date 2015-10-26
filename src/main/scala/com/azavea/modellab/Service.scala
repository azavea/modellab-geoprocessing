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

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global

import spray.http.MediaTypes
import spray.http.StatusCodes
import spray.httpx.encoding._
import spray.httpx.SprayJsonSupport._
import spray.httpx.unmarshalling._
import spray.json._
import spray.routing._


object Service extends SimpleRoutingApp /*with DataHubCatalog*/  with App {
  implicit val system = ActorSystem("spray-system")
  // implicit val sparkContext = SparkUtils.createSparkContext("Catalog")
  // implicit val sc = geotrellis.spark.utils.SparkUtils.createLocalSparkContext("local[*]", "Model Service")
  
  import scala.collection.mutable
  
  val colorBreaks = mutable.HashMap.empty[String, ColorBreaks]

  // val regsitry = new LayerRegistry
  // val parser = new Parser(regsitry, layerReader)

  // // Testing: Auto load some Op definitions.
  // parser.parse(TestNodes.maskCities)
  // parser.parse(TestNodes.maskForest)

  val resize = new ResizeTile(256, 512)
  val buffer = 1

  // def registerLayerRoute = post {
  //   requestInstance { req =>
  //     complete {
  //       val json = req.entity.asString.parseJson
  //       val node = parser.parse(json)
  //       println(s"Registered: $node")        
  //       StatusCodes.Accepted
  //     }
  //   }
  // }

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
            StatusCodes.Accepted
          }
        }
      }
    }

  // def guidRoute = pathPrefix(Segment / IntNumber / IntNumber / IntNumber) { (guid, zoom, x, y) =>
  //   parameters('breaks.?) { breaksName =>
  //     respondWithMediaType(MediaTypes.`image/png`) {
  //       complete{ future {
  //         regsitry.getTile(guid, zoom - 1, x, y)
  //           .map { tile =>
  //             {
  //               for {
  //                 name <- breaksName
  //                 breaks <- colorBreaks.get(name)
  //               } yield tile.renderPng(breaks).bytes
  //             }.getOrElse(tile.renderPng().bytes )
  //           }
  //       } }
  //     }
  //   }
  // }

  def tileRoute = pathPrefix("tile" / Segment / IntNumber / IntNumber / IntNumber) { (expr, zoom, x, y) =>
    parameters('breaks.?) { breaksName =>
      respondWithMediaType(MediaTypes.`image/png`) {
        complete {
          future {
            val requestKey = SpatialKey(x, y)
            val storedKey = resize.getStoredKey(requestKey)
            val SpatialKey(col, row) = storedKey
            val bounds = GridBounds(
              col - col % buffer, row - row % buffer,
              col + (buffer - col % buffer), row + (buffer - row % buffer))

            val fn = RPN.parse(expr, zoom - 1)

            val rdd = fn(bounds)
            val tile = rdd.lookup(storedKey).head
            resize.getTile(requestKey, tile).renderPng().bytes
          }
        }
      }
    }
  }

  startServer(interface = "0.0.0.0", port = 8888) {
    tileRoute /* ~ guidRoute ~ path("register"){registerLayerRoute} */ ~ pathPrefix("breaks"){registerColorBreaksRoute}
  }
}
