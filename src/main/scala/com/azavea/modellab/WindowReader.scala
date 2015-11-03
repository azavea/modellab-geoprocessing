package com.azavea.modellab

import geotrellis.spark._
import spray.json._
import DefaultJsonProtocol._
import geotrellis.raster.io.geotiff._
import geotrellis.raster._
import geotrellis.spark.io._
import org.apache.spark.rdd._
import org.apache.spark.storage._

import scala.collection._
import geotrellis.spark.utils.cache._
import scala.collection.concurrent.TrieMap


/**
 * The purpose of this class is to buffer IO, which benefits from larger windows.
 * 
 * - First stage is going to be to get a large window to share IO.
 * - Second stage is to use the larger window to perform calculation quickly.
 */
class WindowedReader(
  layerReader: FilteringLayerReader[LayerId, SpatialKey, RasterRDD[SpatialKey]], 
  windowSize: Int = 6, 
  storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER) extends Window(windowSize) with Instrumented {

  type K = (LayerId, GridBounds)
  type V = RasterRDD[SpatialKey]
  private val windowCache = new TrieMap[K, V]

  private[this] val cacheHit = metrics.counter("hit")
  private[this] val cacheMiss = metrics.counter("miss")

  def getWindow(id: LayerId, key: SpatialKey): RasterRDD[SpatialKey] = {
    val windowBounds = getWindowBounds(key)
    windowCache.synchronized {
      val k = (id, windowBounds)      
      if ( windowCache.contains(k) ) cacheHit += 1 else cacheMiss += 1
      windowCache.getOrElseUpdate(k,
        layerReader.query(id).where(Intersects(windowBounds)).toRDD.setName(s"Window(${id.name}, ${id.zoom}, $windowBounds)").persist(storageLevel))
    } 
  }

  /** 
   * If we stradle window boundry, we need to find which windows we intercept.
   */
  def getView(id: LayerId, bounds: GridBounds): RasterRDD[SpatialKey] = {
    val windows = {
      for {
        row <- List(bounds.rowMin, bounds.rowMax)
        col <- List(bounds.colMin, bounds.colMax)
      } yield getWindow(id, SpatialKey(col, row))
    }.toSet
    println(s"Request: $id - $bounds intersects: ${windows.size} windows")
    val metaData = windows.head.metaData
    val tilesRDD = windows
      .map { _.filter { case (key, tile) => bounds.contains(key.col, key.row) } }
      .reduce( _ union _)
    new RasterRDD(tilesRDD, metaData)
  }
}


class Window(windowSize: Int) {
  def getWindowId(key: SpatialKey) = {
    val SpatialKey(col, row) = key
    (col / windowSize, row / windowSize)
  }

  def getWindowBounds(key: SpatialKey) = {
    val SpatialKey(col, row) = key
    GridBounds(
      col - col % windowSize, row - row % windowSize,
      col + (windowSize - col % windowSize), row + (windowSize - row % windowSize))
  }   
}
