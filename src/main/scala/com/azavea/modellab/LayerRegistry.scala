package com.azavea.modellab

import geotrellis.spark._
import spray.json._
import DefaultJsonProtocol._
import geotrellis.raster.io.geotiff._
import geotrellis.raster._
import geotrellis.spark.io._
import org.apache.spark.rdd._
import scala.collection.concurrent.TrieMap

class LayerRegistry(layerReader: FilteringLayerReader[LayerId, SpatialKey, RasterRDD[SpatialKey]]) extends Instrumented {
  private val layerCache = new TrieMap[String, Node]

  val formats = new NodeFormats(new WindowedReader(layerReader, 8)) // base layer is read and cached in 8x8 native tiles
  val resize = new ResizeTile(256, 512) // we're reading from DataHub, tiles need to be split to be rendred
  val window = new Window(2)            // buffer operation requests by 2 (storage) tiles each direction  

  def register(json: JsValue): JsObject = {
    import formats._
    val node = json.convertTo[Node]
    registerNodeTree(node)
    node.toJson.asJsObject
  }

  def registerNodeTree(node: Node): Unit = {
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
  
  def getLayer(hash: String): Option[Node] = 
    layerCache.get(hash)

  def getTile(hash: String, zoom: Int, col: Int, row: Int): Option[Tile] = {
    val requestKey = SpatialKey(col, row)
    val storedKey = resize.getStoredKey(requestKey)
    val bounds = window.getWindowBounds(storedKey)    
    getLayer(hash).map { window =>
      val tiles = window(zoom, bounds).lookup(storedKey)
      resize.getTile(requestKey, tiles.head)
    }
  }
}


