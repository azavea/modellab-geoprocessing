package com.azavea.modellab

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io._
import geotrellis.spark.io.index._

import org.apache.spark._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path

trait TestCatalog extends Catalog {
  implicit def sc: SparkContext

  // this catalog does not yet exist, but can be created later with GeoTrellis ETL
  def catalogPath(implicit sc: SparkContext): Path = {
    val localFS = new Path(System.getProperty("java.io.tmpdir")).getFileSystem(sc.hadoopConfiguration)
    new Path(localFS.getWorkingDirectory, "src/test/resources/test-catalog")
  }

  lazy val layerReader = HadoopLayerReader[SpatialKey, Tile, RasterRDD](catalogPath)
  lazy val layerWriter = HadoopLayerWriter[SpatialKey, Tile, RasterRDD](catalogPath, ZCurveKeyIndexMethod)
}