package com.azavea.modellab

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.render._
import geotrellis.raster.resample._
import geotrellis.raster.histogram._
import geotrellis.raster.io.json._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.s3._
import geotrellis.spark.io.avro.codecs._
import geotrellis.spark.io._
import geotrellis.spark.io.index._
import geotrellis.spark.io.json._
import geotrellis.spark.tiling._
import geotrellis.spark.utils.SparkUtils
import geotrellis.vector._
import geotrellis.vector.reproject._

import org.apache.hadoop.fs.Path
import org.apache.spark._
import org.apache.spark.rdd._

trait Catalog {
  implicit def sc: SparkContext
  
  def layerReader: FilteringLayerReader[LayerId, SpatialKey, RasterRDD[SpatialKey]] 
  def layerWriter : Writer[LayerId, RasterRDD[SpatialKey] with RDD[(SpatialKey, Tile)]]
}

trait DataHubCatalog extends Catalog {
  implicit def sc: SparkContext

  private val bucket = "azavea-datahub"
  private val key = "catalog"

  val cache = (id: LayerId) => new FileCache(s"/Users/eugene/tmp/model-lab/cache/${id.name}/${id.zoom}", _.toString)

  lazy val layerReader = new S3LayerReader[SpatialKey, Tile, RasterRDD[SpatialKey]](
      new S3AttributeStore(bucket, key),
      new S3RDDReader[SpatialKey, Tile],
      None) {
    override val defaultNumPartitions = math.max(1, sc.defaultParallelism)

    override def read(id: LayerId, rasterQuery: RDDQuery[SpatialKey, MetaDataType], numPartitions: Int): RasterRDD[SpatialKey] = {
      println(s"layerReader.read($id)")
      super.read(id, rasterQuery, numPartitions)
    }
  }

  lazy val layerWriter = S3LayerWriter[SpatialKey, Tile, RasterRDD](bucket, key, ZCurveKeyIndexMethod)

}

trait TestCatalog extends Catalog {
  implicit def sc: SparkContext

  val catalogPath = new Path("file:/Users/eugene/tmp/model-lab/catalog")
  lazy val layerReader = HadoopLayerReader[SpatialKey, Tile, RasterRDD](catalogPath)
  lazy val layerWriter = HadoopLayerWriter[SpatialKey, Tile, RasterRDD](catalogPath, ZCurveKeyIndexMethod)
}