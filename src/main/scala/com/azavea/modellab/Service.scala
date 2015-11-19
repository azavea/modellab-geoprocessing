package com.azavea.modellab

import akka.actor.ActorSystem

import geotrellis.raster._
import geotrellis.raster.render._
import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global

import spray.http._
import spray.httpx.encoding._
import spray.httpx.unmarshalling._
import spray.httpx.marshalling._
import spray.httpx.SprayJsonSupport._
import spray.util.LoggingContext
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

  def registerLayerRoute = {
    import DefaultJsonProtocol._
    post {
      requestInstance { req =>
        respondWithHeader(RawHeader("Access-Control-Allow-Origin", "*")) {
          complete {
            val requestJson = req.entity.asString.parseJson
            val renderedJson = registry.register(requestJson)
            JsonMerge(renderedJson, requestJson).asJsObject
          }
        }
      } 
    } ~ 
    get {
      pathPrefix(Segment) { layerHash =>
        complete{          
          registry.getLayerJson(layerHash) 
        }
      } ~
      pathEnd {
        complete {
          JsObject("layers" -> registry.listLayers.toJson)
        }
      }
    }
  }

  def registerColorBreaksRoute =
    pathPrefix(Segment) { breaksName =>
      post {
        requestInstance { req =>
          respondWithHeader(RawHeader("Access-Control-Allow-Origin", "*")) {
            complete {
              import java.math.BigInteger

              val blob = req.entity.asString
              val breaks = {
                val split = blob.split(";").map(_.trim.split(":"))
                logger.debug(split.toList.toString)
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
    }


  def renderRoute = pathPrefix(Segment / IntNumber / IntNumber / IntNumber) { (hash, zoom, x, y) =>
    parameters('breaks.?) { breaksName =>
      respondWithMediaType(MediaTypes.`image/png`) {
        complete{ 
          def render(tile: Tile): Array[Byte] = {
            for {
              name <- breaksName
              breaks <- colorBreaks.get(name)
            } yield tile.renderPng(breaks).bytes
          }.getOrElse(tile.renderPng().bytes)

          for { optionFutureTile <- registry.getTile(hash, zoom, x, y) } yield
            for { optionTile <- optionFutureTile }  yield 
              for (tile <- optionTile) yield render(tile)                               
        }
      }
    }
  }

  def valueRoute = pathPrefix(Segment / IntNumber / IntNumber / IntNumber) { (hash, zoom, x, y) =>
    import DefaultJsonProtocol._
    complete{
      for { optionFutureTile <- registry.getTile(hash, zoom, x, y) } yield
        for { optionTile <- optionFutureTile }  yield
          for (tile <- optionTile) yield renderAsValueGrid(tile)
    }
  }

  lazy val sampleReader = new SampleNeighborhood(registry.getTile)
  def sampleRoute = pathPrefix(Segment / IntNumber / DoubleNumber / DoubleNumber) { (hash, zoom, lng, lat) =>
    parameters('bufferSize.as[Int] ? 10) { bufferSize =>
      import DefaultJsonProtocol._
      complete {
        sampleReader.sample(hash, zoom, lng, lat, bufferSize).map(tile => renderAsValueGrid(tile))
      }
    }
  }

  startServer(interface = "0.0.0.0", port = 8888) {
    handleExceptions(exceptionHandler) {
      pathPrefix("tms") {
        renderRoute ~
        pathPrefix("value") { valueRoute }
      } ~
      pathPrefix("sample") {
        sampleRoute
      } ~
      pingPong ~
      pathPrefix("layers"){registerLayerRoute} ~
      pathPrefix("breaks"){registerColorBreaksRoute}
    }
  }

  def exceptionHandler(implicit log: LoggingContext) = ExceptionHandler {
    case e: Exception =>
      requestUri { uri =>
        log.warning("Request to {} could not be handled normally", uri)
        e.printStackTrace()
        complete(StatusCodes.InternalServerError, s"$e")
      }
  }


  def renderAsValueGrid(tile: Tile) = JsArray(
    {for (r <- 0 to tile.rows) yield
    JsArray(
      {for (c <- 0 to tile.cols) yield
      if (tile.cellType.isFloatingPoint) {
        val v = tile.getDouble(c, r)
        if (isNoData(v)) JsNull else JsNumber(v)
      } else {
        val v = tile.get(c, r)
        if (isNoData(v)) JsNull else JsNumber(v)
      }
      }.toVector
    )
    }.toVector
  )
}