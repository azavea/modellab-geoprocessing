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
  val cache = mutable.HashMap.empty[(Node, Int, GridBounds), RasterRDD[SpatialKey]]
}

trait Node extends Serializable {
  def calc(zoom: Int, bounds: GridBounds): RasterRDD[SpatialKey]

  def apply(zoom: Int, bounds: GridBounds): RasterRDD[SpatialKey] = {
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

case class FocalOp(
  op: (Tile, Neighborhood, Option[GridBounds]) => Tile,
  input: Node,
  n: Neighborhood
) extends Node {
  require(n.extent <= 256, "Maximum Neighborhood extent is 256 cells")
  def buffer(bounds: GridBounds, cells: Int) = GridBounds(
    math.max(0, bounds.colMin - cells),
    math.max(0, bounds.rowMin - cells),
    bounds.colMax + cells,
    bounds.rowMax + cells)

  def calc(zoom: Int, bounds: GridBounds) = {
    val rasterRDD = input(zoom, bounds)
    val metaData = rasterRDD.metaData
    val extended = buffer(bounds, 1)
    println(s"FOCAL: $bounds -> $extended")
    val tileRDD = FocalOperation(rasterRDD, n, Some(bounds))(op)
      .filter { case (key, _) => bounds.contains(key.col, key.row) } // filter buffer tiles, they contain no information
    new RasterRDD(tileRDD, metaData)
  }
}

case class AspectOp(
  op: (Tile, Neighborhood, Option[GridBounds], CellSize) => Tile,
  input: Node,
  n: Neighborhood
) extends Node {
  def calc(zoom: Int, bounds: GridBounds) = {
    val rasterRDD = input(zoom, bounds)
    val metaData = rasterRDD.metaData
    val cs = metaData.layout.rasterExtent.cellSize
    val tileRDD = rasterRDD map { case (key, tile) => key -> op(tile, n, None, cs) }
    new RasterRDD(tileRDD, metaData)
  }
}

case class SlopeOp(
  op: (Tile, Neighborhood, Option[GridBounds], CellSize, Double) => Tile,
  input: Node,
  n: Neighborhood,
  z: Double
) extends Node {
  def calc(zoom: Int, bounds: GridBounds) = {
    val rasterRDD = input(zoom, bounds)
    val metaData = rasterRDD.metaData
    val cs = metaData.layout.rasterExtent.cellSize
    val tileRDD = rasterRDD map { case (key, tile) => key -> op(tile, n, None, cs, z) }
    new RasterRDD(tileRDD, metaData)
  }
}

case class LocalUnaryOp(
  op: Function1[Tile, Tile],
  input: Node
) extends Node {
  def calc(zoom: Int, bounds: GridBounds) = {
    val rasterRDD = input(zoom, bounds)
    val metaData = rasterRDD.metaData
    val tileRDD = rasterRDD map { case (key, tile) => key -> op(tile) }
    new RasterRDD(tileRDD, metaData)
  }
}

case class LocalBinaryOp(
  op: LocalTileBinaryOp,
  input: Seq[Node],
  const: Option[Double] = None // Constant value
) extends Node {
  def calc(zoom: Int, bounds: GridBounds) = {
    val metaData = input.head(zoom, bounds).metaData
    val tileRDD = input
      .map { _(zoom, bounds).tileRdd }
      .reduce { _ union _ }
      .reduceByKey(
        (aggr: Tile, value: Tile) => op(aggr, value)
      )

    val outTile =
      const match {
        case Some(constant) => {  // Have a constant
          metaData.cellType.isFloatingPoint match {  // Constant is floating point
            case true => tileRDD.map { case (key, tile) => key -> op(tile, constant.toDouble) }
            case false => tileRDD.map { case (key, tile) => key -> op(tile, constant) }
          }
        }
        case _ => tileRDD  // No constant provided
      }

    new RasterRDD(outTile, metaData)
  }
}

case class MapValuesOp(
  input: Node,
  mappings: Seq[(Seq[Int], Option[Int])]
) extends Node {
  def calc(zoom: Int, bounds: GridBounds) = {
    val rasterRDD = input(zoom, bounds)
    val metaData = rasterRDD.metaData
    require(!metaData.cellType.isFloatingPoint, "Only discrete (integer) tiles can map by value")
    val map = mappings.flatMap { tuple: (Seq[Int], Option[Int]) =>
        tuple._1 map { _ -> tuple._2 }
      }.toMap

    val tileRDD = rasterRDD map { case (key, tile) =>
      key -> tile.map(map(_).getOrElse(NODATA))
    }
    new RasterRDD(tileRDD, metaData)
  }
}

