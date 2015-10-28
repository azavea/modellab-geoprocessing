package com.azavea.modellab

import spray.json._
import spray.json.DefaultJsonProtocol._
import shapeless._
import shapeless.ops.hlist._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.op.focal._
import geotrellis.raster._
import geotrellis.raster.op.local._
import geotrellis.raster.op.focal._
import org.apache.spark.storage._
import scala.collection.mutable
import org.apache.spark.rdd._

object Node {
  val cache = mutable.HashMap.empty[(Node, Int, GridBounds), RDD[(SpatialKey, Tile)]]
}

trait Node extends Serializable {
  def calc(zoom: Int, bounds: GridBounds): RDD[(SpatialKey, Tile)]

  def apply(zoom: Int, bounds: GridBounds): RDD[(SpatialKey, Tile)] = {
    // This will backstop calculation of RDDs nodes, hopefully allowing IO nodes to be re-used
    // Critical note: the Node tree will already exist fully.
    val name = s"${this.getClass.getName}::$zoom::$bounds"
    val key = (this, zoom, bounds)
    
    // NOTE: We used to cache here to store every layer and their intermidiate results
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

case class LoadLayer(layerName: String, layerReader: WindowedReader) extends Node { 
  def calc(zoom: Int, bounds: GridBounds) = {
    layerReader.getView(LayerId(layerName, zoom), bounds)
  }
}

case class FocalOp(layer: Node, n: Neighborhood, op: (Tile, Neighborhood, Option[GridBounds]) => Tile) extends Node {
  require(n.extent <= 256, "Maximum Neighborhood extent is 256 cells")
  def buffer(bounds: GridBounds, cells: Int) = GridBounds(
    math.max(0, bounds.colMin - cells), 
    math.max(0, bounds.rowMin - cells),
    bounds.colMax + cells, 
    bounds.rowMax + cells)

  def calc(zoom: Int, bounds: GridBounds) = {
    val extended = buffer(bounds, 1)
    println(s"FOCAL: $bounds -> $extended")
    val rdd = layer(zoom, extended)
    FocalOperation(rdd, Square(1), Some(bounds))(geotrellis.raster.op.focal.Min.apply)
      .filter { case (key, _) => bounds.contains(key.col, key.row) } // filter buffer tiles, they contain no information
  }
}

case class LocalUnaryOp(op: Function1[Tile, Tile], input: Node) extends Node {
  def calc(zoom: Int, bounds: GridBounds) = {
    input(zoom, bounds) map { case (key, tile) => key -> op(tile) }
  }
}

case class LocalBinaryOp(op: LocalTileBinaryOp, input: Seq[Node], const: Option[Int] = None) extends Node {
  def calc(zoom: Int, bounds: GridBounds) = {
    input map { _(zoom, bounds) } reduce { _ union _ } combineByKey(
      (init: Tile) => init,
      (aggr: Tile, value: Tile) => op(aggr, value),
      (aggr: Tile, value: Tile) => op(aggr, value)
    )
  }
}

case class MappingOp(input: Node, mapFrom: Seq[Int], mapTo: Seq[Int]) extends Node {
  def calc(zoom: Int, bounds: GridBounds) = {
    val mappings = (mapFrom zip mapTo).toMap
    input(zoom, bounds) map { case (key, tile) =>
      key -> tile.map(mappings)
    }
  }
}

case class ValueMask(a: Node, masks: Seq[Int]) extends Node {
  def calc(zoom: Int, bounds: GridBounds) = {
    val _masks = masks
    a(zoom, bounds).map { case (key, tile) =>
      key -> tile.map { v => if (_masks.contains(v)) NODATA else v }
    }
  }
}
