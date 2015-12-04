package com.azavea.modellab

import com.azavea.modellab.op._
import geotrellis.spark._
import spray.json._
import geotrellis.raster._
import geotrellis.spark.io._
import scala.collection.concurrent.TrieMap
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global

import com.typesafe.config._

class LayerRegistry(layerReader: FilteringLayerReader[LayerId, SpatialKey, RasterRDD[SpatialKey]]) extends Instrumented {
  private val layerCache = new TrieMap[String, Op]

  val config = ConfigFactory.load()
  val windowSize = config.getInt("cache.windowSize")
  val opSize = config.getInt("cache.opSize")

  val formats = new NodeFormats(new WindowedReader(layerReader, windowSize), getLayer) // base layer is read and cached in 8x8 native tiles
  val resize = new ResizeTile(256, 512) // we're reading from DataHub, tiles need to be split to be rendred
  val window = new Window(opSize)            // buffer operation requests by 2 (storage) tiles each direction

  private[this] lazy val collectTimer = metrics.timer("collect")

  def register(json: JsValue): JsObject = {
    import formats._
    val node = json.convertTo[Op]
    registerNodeTree(node)
    node.toJson.asJsObject
  }

  def registerNodeTree(node: Op): Unit = {
    val hash = node.hashString

    layerCache.putIfAbsent(hash,
      {
        logger.info(s"Register: $hash == $node")
        node
      })

    for (input <- node.inputs)
      registerNodeTree(input)
  }

  def getLayerJson(hash: String): Option[JsObject] = {
    import formats._
    layerCache.get(hash).map(_.toJson.asJsObject)
  }

  def getLayer(hash: String): Option[Op] =
    layerCache.get(hash)

  def listLayers: Seq[String] =
    layerCache.keys.toSeq

  import geotrellis.spark.utils.cache._

  private val tileCache = new LRUCache[(String, GridBounds), Future[Array[(SpatialKey, Tile)]]](100L, _ => 1L) //cache last 100 windows

  def getTile(hash: String, zoom: Int, col: Int, row: Int): Option[Future[Option[Tile]]] = {
    for { layer <- getLayer(hash) } yield {
      val requestKey = SpatialKey(col, row)
      val storedKey = resize.getStoredKey(requestKey)
      val bounds = window.getWindowBounds(storedKey)

      val futureTiles: Future[Array[(SpatialKey, Tile)]] =
        tileCache.synchronized {
          val key = (hash, bounds)
          // this funny way doing it as required because Cache.getOrInsert will double eval the value, causing two futures to be spawned
          tileCache.lookup(key) match {
            case Some(ft) => ft
            case None =>
              val ft = future {
                collectTimer.time {
                  layer(zoom - resize.zoomOffset, bounds).collect
                }
              }
              tileCache.insert(key, ft)
              ft
          }
        }

      futureTiles.map { tiles: Array[(SpatialKey, Tile)] =>
        for { (key, tile) <- tiles.find( _._1 == storedKey) }
        yield resize.getTile(requestKey, tile)
      }
    }
  }
}


