package com.azavea.modellab

import spray.json._
import spray.json.DefaultJsonProtocol._
import shapeless._
import shapeless.ops.hlist._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.op.focal._
import geotrellis.raster.op.elevation._
import geotrellis.raster._
import geotrellis.raster.op.local._
import geotrellis.raster.op.focal._
import org.apache.spark.storage._
import scala.collection.mutable
import org.apache.spark.rdd._
import java.nio.ByteBuffer
import org.apache.commons.codec.binary._
import scala.collection.concurrent.TrieMap

object Node {
  type K = (Node, Int, GridBounds)
  type V = RasterRDD[SpatialKey]
  val cache = new TrieMap[K,V]
}

trait Node extends Serializable with Instrumented {  
  private[this] val cacheHit = metrics.counter("cache.hit")
  private[this] val cacheMiss = metrics.counter("cache.miss")

  def calc(zoom: Int, bounds: GridBounds): RasterRDD[SpatialKey]

  def apply(zoom: Int, bounds: GridBounds): RasterRDD[SpatialKey] = {
    // This will backstop calculation of RDDs nodes, hopefully allowing IO nodes to be re-used
    // Critical note: the Node tree will already exist fully.
    val key = (this, zoom, bounds)
    val name = s"${this.getClass.getSimpleName}($zoom, $bounds)"

    // NOTE: We used to cache here to store every layer and their intermidiate results
    // We actually disable the cache because it appears caching result tiles in LRU cache on the driver is more efficient
    //  likely there will be cases where that is not true, benchmarking may change this conclusion
    
    // Node.cache.synchronized {
    //   if ( Node.cache.contains(key) ) cacheHit += 1 else cacheMiss += 1      
    //   Node.cache.getOrElseUpdate(key, calc(zoom, bounds).setName(name).cache)
    // }
    calc(zoom, bounds).setName(name)
  }

  def inputs: Seq[Node]

  def hashString: String = {
    val buff = ByteBuffer.allocate(4);
    buff.putInt(hashCode)
    //32 bit int in base 64 is always going to be padded with '=='
    Base64.encodeBase64(buff.array).map(_.toChar).mkString.substring(0,6)
  }
}

case class LoadLayer(layerName: String, layerReader: WindowedReader) extends Node {
  def calc(zoom: Int, bounds: GridBounds) = {
    layerReader.getView(LayerId(layerName, zoom), bounds)
  }

  def inputs = Seq.empty

  override def hashCode = layerName.hashCode

  override def toString = s"LoadLayer($layerName)"
}

case class FocalOp(
  name: String,
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
    val _op = op
    val extended = buffer(bounds, 1)
    val rasterRDD = input(zoom, extended)
    val metaData = rasterRDD.metaData
    val tileRDD = FocalOperation(rasterRDD, n, Some(bounds))(_op)
      .filter { case (key, _) => bounds.contains(key.col, key.row) } // filter buffer tiles, they contain no information
    new RasterRDD(tileRDD, metaData)
  }

  def inputs = Seq(input)

  override def hashCode = (name, n, input).hashCode
  override def toString = s"FocalOp($name, ${n.getClass.getSimpleName}, $input)"
}

case class AspectOp(
  input: Node,
  n: Neighborhood
) extends Node {
  def calc(zoom: Int, bounds: GridBounds) = {
    val rasterRDD = input(zoom, bounds)
    val cs = rasterRDD.metaData.layout.rasterExtent.cellSize
    rasterRDD.mapTiles { tile => Aspect(tile, n, None, cs) }    
  }

  def inputs = Seq(input)

  override def hashCode = ("Aspect", input, n).hashCode
  override def toString = s"AspectOp(${n.getClass.getSimpleName}, $input)"
}

case class SlopeOp(
  input: Node,
  n: Neighborhood,
  z: Double
) extends Node {
  def calc(zoom: Int, bounds: GridBounds) = {
    val rasterRDD = input(zoom, bounds)
    val cs = rasterRDD.metaData.layout.rasterExtent.cellSize
    rasterRDD.mapTiles { tile => Slope(tile, n, None, cs, z) }
  }

  def inputs = Seq(input)
  
  override def hashCode = ("Slope", input, n, z).hashCode
  override def toString = s"SlopeOp(${n.getClass.getSimpleName}, $input)"
}

case class LocalUnaryOp(
  name: String,
  op: Function1[Tile, Tile],
  input: Node
) extends Node {
  def calc(zoom: Int, bounds: GridBounds) = {
    val _op = op
    input(zoom, bounds).mapTiles(_op)
  }

  def inputs = Seq(input)
  
  override def hashCode = name.hashCode + input.hashCode
  override def toString = s"LocalUnaryOp($name, $input)"
}

case class LocalBinaryOp(
  op: LocalTileBinaryOp,
  inputs: Seq[Node],
  const: Option[Double] = None // Constant value
) extends Node {
  def calc(zoom: Int, bounds: GridBounds) = {    
    val _op = op
    val metaData = inputs.head(zoom, bounds).metaData
    val tileRDD = inputs
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

  override def hashCode = (op.name, inputs, const).hashCode  
  override def toString = s"LocalBinaryOp(${op.name}, $const, $inputs)"
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

  def inputs = Seq(input)

  override def hashCode = (input, mappings).hashCode  
}

