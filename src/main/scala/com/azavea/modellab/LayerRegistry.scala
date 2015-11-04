package com.azavea.modellab

import geotrellis.spark._
import spray.json._
import DefaultJsonProtocol._
import geotrellis.raster.io.geotiff._
import geotrellis.raster._
import geotrellis.spark.io._
import org.apache.spark.rdd._

import scala.collection.mutable
import scala.collection.concurrent

class LayerRegistry {
  type GUID = String

  private val layerCache = concurrent.TrieMap.empty[GUID, Node]

  def registerLayer(guid: GUID, layer: Node): Node = {
    println(s"Registred: $guid")
    layerCache.update(guid, layer)
    layer
  }

  def getLayer(guid: GUID): Option[Node] = layerCache.get(guid)

  def buffer(key: SpatialKey, buffer: Int) = {
    val SpatialKey(col, row) = key
    GridBounds(
      col - col % buffer, row - row % buffer,
      col + (buffer - col % buffer), row + (buffer - row % buffer))
  }

  val resize = new ResizeTile(256, 512) // we're reading from DataHub, tiles are too big
  val window = new Window(2)            // bucffer operation requests by 2 (storage) tiles each direction  

  def getTile(guid: GUID, zoom: Int, col: Int, row: Int): Option[Tile] = {
    val requestKey = SpatialKey(col, row)
    val storedKey = resize.getStoredKey(requestKey)
    val bounds = window.getWindowBounds(storedKey)
    getLayer(guid).map { window =>
      val tiles = window(zoom, bounds).lookup(storedKey)
      resize.getTile(requestKey, tiles.head)
    }
  }
}


