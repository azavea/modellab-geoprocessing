package com.azavea.modellab

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.render._
import geotrellis.raster.resample._
import geotrellis.raster.histogram._
import geotrellis.raster.io.json._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.s3._
import geotrellis.spark.io.avro.codecs._
import geotrellis.spark.io._
import geotrellis.spark.io.json._
import geotrellis.spark.tiling._
import geotrellis.spark.utils.SparkUtils
import geotrellis.vector._
import geotrellis.vector.reproject._

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global

import akka.actor.ActorSystem

import spray.http.MediaTypes
import spray.httpx.SprayJsonSupport._
import spray.json._
import spray.routing._


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