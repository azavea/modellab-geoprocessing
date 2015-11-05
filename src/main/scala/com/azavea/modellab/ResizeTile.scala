package com.azavea.modellab

import geotrellis.raster._
import geotrellis.spark._

class ResizeTile(requestSize: Int, storedSize: Int) {
  require(requestSize <= storedSize, "storedSize may not be smaller than request size")
  require(storedSize % requestSize == 0, "requestSize must be a multiple of storedSize" )
  val factor = storedSize / requestSize
  val zoomOffset = (math.log(factor) / math.log(2)).toInt
  
  def getStoredLayerId(id: LayerId): LayerId = {
    LayerId(id.name, id.zoom - zoomOffset)
  }

  def getStoredKey(requestKey: SpatialKey) = {
    val SpatialKey(col, row) = requestKey
    SpatialKey(col / factor, row / factor)
  }

  def getTile(requestKey: SpatialKey, storedTile: Tile) = {
    val SpatialKey(col: Int, row: Int) = requestKey
    val tiles: Seq[Tile] = CompositeTile(storedTile, TileLayout(factor, factor, requestSize, requestSize)).tiles
    val index = col % factor + row % factor * 2
    tiles(index)
  } 
}