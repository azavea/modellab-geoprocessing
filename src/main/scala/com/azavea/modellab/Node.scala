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
import java.nio.ByteBuffer
import org.apache.commons.codec.binary._
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import geotrellis.spark.utils.cache._

object Node {
  type K = (Node, Int, GridBounds)
  type V = RasterRDD[SpatialKey]
  val cache = new HashBackedCache[K, V] with LoggingCache[K, V] with AtomicCache[K, V] 
}

trait Node extends Serializable with Instrumented {
  private[this] val logger = LoggerFactory.getLogger(this.getClass);
  private[this] val reading = metrics.timer("calc", this.getClass.getName)

  def calc(zoom: Int, bounds: GridBounds): RasterRDD[SpatialKey]

  def apply(zoom: Int, bounds: GridBounds): RasterRDD[SpatialKey] = {
    // This will backstop calculation of RDDs nodes, hopefully allowing IO nodes to be re-used
    // Critical note: the Node tree will already exist fully.
    val name = s"${this.getClass.getName}::$zoom::$bounds"
    val key = (this, zoom, bounds)

    // NOTE: We used to cache here to store every layer and their intermidiate results
    Node.cache.getOrInsert(key, calc(zoom, bounds).setName(name).cache)
  }

  def hash: String = {
    val buff = ByteBuffer.allocate(8);
    buff.putInt(hashCode)
    Base64.encodeBase64(buff.array).mkString
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
    val _op = op
    input(zoom, bounds).mapTiles(_op)
  }
}

case class LocalBinaryOp(
  op: LocalTileBinaryOp,
  input: Seq[Node],
  const: Option[Double] = None // Constant value
) extends Node {
  def calc(zoom: Int, bounds: GridBounds) = {    
    val _op = op
    val metaData = input.head(zoom, bounds).metaData
    val tileRDD = input
      .map { _(zoom, bounds).tileRdd }
      .reduce { _ union _ }
      .reduceByKey(
        (aggr: Tile, value: Tile) => _op(aggr, value)
      )

    val outTile =
      const match {
        case Some(constant) => {  // Have a constant
          metaData.cellType.isFloatingPoint match {  // Constant is floating point
            case true => tileRDD.map { case (key, tile) => key -> _op(tile, constant.toDouble) }
            case false => tileRDD.map { case (key, tile) => key -> _op(tile, constant) }
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
      key -> tile.map { cellValue => 
        // if cellValue is not mentioned in map, map to itself, if it maps to None, map to NODATA
        map.getOrElse(cellValue, Some(cellValue)).getOrElse(NODATA)
      }
    }
    new RasterRDD(tileRDD, metaData)
  }
}

