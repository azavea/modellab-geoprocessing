package com.azavea.modellab

import spray.json._
import spray.json.DefaultJsonProtocol._
import shapeless._
import shapeless.ops.hlist._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.raster._
import geotrellis.raster.op.local._

import org.apache.spark.storage._
import scala.collection.mutable

object Node {
  val cache = mutable.HashMap.empty[(Node, Int, GridBounds), RasterRDD[SpatialKey]]
}

trait Node extends Serializable {
  def calc(zoom: Int, bounds: GridBounds): RasterRDD[SpatialKey]

  def apply(zoom: Int, bounds: GridBounds): RasterRDD[SpatialKey] = {
    // This will backstop calculation of RDDs nodes, hopefully allowing IO nodes to be re-used
    // Critical note: the Node tree will already exist fully.
    val name = s"${this.getClass.getName}::$zoom::$bounds"
    val key = (this, zoom, bounds)
    Node.cache.get(key) match {
      case Some(rdd) =>
        println(s"Pulled Node: $name")    
        rdd

      case None =>
        val rdd = 
          calc(zoom, bounds)
            .setName(s"${this.getClass.getName}::$zoom::$bounds")
            .cache
        Node.cache.update(key, rdd)
        println(s"PUSHED Node: $name")  
        rdd          
    }
  }
}

case class LoadLayer(layerName: String, layerReader: FilteringLayerReader[LayerId, SpatialKey, RasterRDD[SpatialKey]]) extends Node { 
  def calc(zoom: Int, bounds: GridBounds) = {
    val rdd = layerReader.query(LayerId(layerName, zoom)).where(Intersects(bounds)).toRDD
    rdd.foreachPartition{ p => () }
    rdd
  }
}

case class LocalAdd(a: Node, b: Node) extends Node {
  def calc(zoom: Int, bounds: GridBounds) = {
    a(zoom, bounds).combineTiles(b(zoom, bounds)) { Add(_,_) }
  }
}

case class ValueMask(a: Node, masks: Seq[Int]) extends Node {
  def calc(zoom: Int, bounds: GridBounds) = {
    val _masks = masks
    a(zoom, bounds).mapTiles { tile =>
      tile.map { v => if (_masks.contains(v)) NODATA else v }
    }
  }    
}