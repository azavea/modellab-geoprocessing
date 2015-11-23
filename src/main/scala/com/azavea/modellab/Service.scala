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

import com.amazonaws.AmazonClientException

object StaticConfig {
  import java.lang.Class
  import scala.io.Source

  def resourceToString(resource : String) = {
    Source.fromInputStream(getClass.getResourceAsStream(resource)).mkString
  }

  val layers = List(
    "/layers/canopy2001.json",
    "/layers/census2010popsqmi.json",
    "/layers/census2010popsqmiQuantile.json",
    "/layers/census2010totalpop.json",
    "/layers/census2010totalpopQuantile.json",
    "/layers/census2013income.json",
    "/layers/census2013incomeQuantile.json",
    "/layers/elevation.json",
    "/layers/impervious2001.json",
    "/layers/impervious2011.json",
    "/layers/landcover2006.json",
    "/layers/landcover2011.json",
    "/layers/landsatBlue.json",
    "/layers/landsatGreen.json",
    "/layers/landsatNir.json",
    "/layers/landsatRed.json",
    "/layers/nlcd.json",
    "/layers/reclass2011.json",
    "/layers/trees2011.json",
    "/layers/water2011.json"
  )
    .map(resourceToString)
    .map { str : String => str.parseJson }

  val breaks = List(
    "black-0-1",
    "black-0-100",
    "blue-0-1",
    "blue-0-100",
    "cm",
    "dosc",
    "elevation",
    "green-0-1",
    "green-0-100",
    "landsat",
    "nlcd",
    "nlcd-reclass",
    "purple-0-100000",
    "red-0-1000",
    "rosc",
    "tpi",
    "tr"
  ).zip(List(
    "/breaks/black-0-1.data",
    "/breaks/black-0-100.data",
    "/breaks/blue-0-1.data",
    "/breaks/blue-0-100.data",
    "/breaks/cm.data",
    "/breaks/dosc.data",
    "/breaks/elevation.data",
    "/breaks/green-0-1.data",
    "/breaks/green-0-100.data",
    "/breaks/landsat.data",
    "/breaks/nlcd.data",
    "/breaks/nlcd-reclass.data",
    "/breaks/purple-0-100000.data",
    "/breaks/red-0-1000.data",
    "/breaks/rosc.data",
    "/breaks/tpi.data",
    "/breaks/tr.data"
  ).map(resourceToString))
}

object Service extends SimpleRoutingApp with DataHubCatalog with Instrumented with App {
  private[this] lazy val requestTimer = metrics.timer("tileRequest")

  implicit val system = ActorSystem("spray-system")
  implicit val sc = geotrellis.spark.utils.SparkUtils.createLocalSparkContext("local[*]", "Model Service")

  val colorBreaks = mutable.HashMap.empty[String, ColorBreaks]

  val registry = try {
    new LayerRegistry(layerReader)
  } catch {
    case e: AmazonClientException =>
      system.shutdown()
      throw new AmazonClientException(e.getMessage())
  }

  def registerBreak(breaksName : String, blob : String) = {
    import java.math.BigInteger

    val breaks = {
      val split = blob.split(";").map(_.trim.split(":"))
      val limits = split.map(pair => Integer.parseInt(pair(0)))
      val colors = split.map(pair => new BigInteger(pair(1), 16).intValue())
      ColorBreaks(limits, colors)
    }
    colorBreaks.update(breaksName, breaks)
    println(s"Registered Breaks: $breaksName")
  }

  // Register static configuration
  StaticConfig.layers.map { x => registry.register(x) }
  StaticConfig.breaks.map { x => registerBreak(x._1, x._2) }

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
              registerBreak(breaksName, req.entity.asString)
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
