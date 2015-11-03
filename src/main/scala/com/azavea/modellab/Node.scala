package com.azavea.modellab

import spray.json._
import spray.json.DefaultJsonProtocol._
import shapeless._
import shapeless.record._
import shapeless.ops.hlist._
import shapeless.syntax.singleton._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.op.focal._
import geotrellis.raster._
import geotrellis.raster.op.local._
import geotrellis.raster.op.focal._
import org.apache.spark.storage._
import scala.collection.mutable
import org.apache.spark.rdd._


// A case class for hanging onto extra parameters
case class NodeParameters(
  neighborhood: Option[Neighborhood] = None,
  layerName: Option[String] = None,
  constant: Option[Double] = None,
  zFactor: Option[Double] = None,
  mappings: Option[Seq[(Seq[Int], Option[Int])]] = None
)

object Node {
  val cache = mutable.HashMap.empty[(Node, Int, GridBounds), RasterRDD[SpatialKey]]
}

trait Node extends Serializable {
  def inputs: Seq[Node]

  def parameters: NodeParameters

  def opName: String

  def id: Int = this.hashCode

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

trait TerminalNode extends Node {
  def inputs = Seq()
}

case class LoadLayerOp(layerName: String, layerReader: WindowedReader) extends TerminalNode {
  override def hashCode = layerName.hashCode
  val opName = this.getClass.getName
  def parameters = NodeParameters(layerName=Some(layerName))

  def calc(zoom: Int, bounds: GridBounds) = {
    layerReader.getView(LayerId(layerName, zoom), bounds)
  }
}

case class FocalOp(
  op: (Tile, Neighborhood, Option[GridBounds]) => Tile,
  opName: String,
  input: Node,
  n: Neighborhood
) extends Node {
  require(n.extent <= 256, "Maximum Neighborhood extent is 256 cells")
  def parameters = NodeParameters(neighborhood=Some(n))
  val _op = op

  override def hashCode: Int = {
    41 * (
      41 * (
        41 + opName.hashCode
      ) + input.hashCode
    ) + n.hashCode
  }

  def inputs = Seq(input)

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
    val tileRDD = FocalOperation(rasterRDD, n, Some(bounds))(_op)
      .filter { case (key, _) => bounds.contains(key.col, key.row) } // filter buffer tiles, they contain no information
    new RasterRDD(tileRDD, metaData)
  }
}

case class AspectOp(
  op: (Tile, Neighborhood, Option[GridBounds], CellSize) => Tile,
  opName: String,
  input: Node,
  n: Neighborhood
) extends Node {
  def inputs = Seq(input)
  def parameters = NodeParameters(neighborhood=Some(n))
  val _op = op

  override def hashCode: Int = {
    41 * (
      41 * (
        41 + opName.hashCode
      ) + input.hashCode
    ) + n.hashCode
  }

  def calc(zoom: Int, bounds: GridBounds) = {
    val rasterRDD = input(zoom, bounds)
    val metaData = rasterRDD.metaData
    val cs = metaData.layout.rasterExtent.cellSize
    val tileRDD = rasterRDD map { case (key, tile) => key -> _op(tile, n, None, cs) }
    new RasterRDD(tileRDD, metaData)
  }
}

case class SlopeOp(
  op: (Tile, Neighborhood, Option[GridBounds], CellSize, Double) => Tile,
  opName: String,
  input: Node,
  n: Neighborhood,
  z: Double
) extends Node {
  def inputs = Seq(input)
  def parameters = NodeParameters(neighborhood=Some(n), zFactor=Some(z))
  val _op = op

  override def hashCode: Int = {
    41 * (
      41 * (
        41 * (
          41 + opName.hashCode
        ) + input.hashCode
      ) + n.hashCode
    ) + z.hashCode
  }


  def calc(zoom: Int, bounds: GridBounds) = {
    val rasterRDD = input(zoom, bounds)
    val metaData = rasterRDD.metaData
    val cs = metaData.layout.rasterExtent.cellSize
    val tileRDD = rasterRDD map { case (key, tile) => key -> _op(tile, n, None, cs, z) }
    new RasterRDD(tileRDD, metaData)
  }
}

case class LocalUnaryOp(
  op: Function1[Tile, Tile],
  opName: String,
  input: Node
) extends Node {
  def inputs = Seq(input)
  def parameters = NodeParameters()
  val _op = op

  override def hashCode: Int = {
    41 * (
      41 + opName.hashCode
    ) + input.hashCode
  }


  def calc(zoom: Int, bounds: GridBounds) = {
    val rasterRDD = input(zoom, bounds)
    val metaData = rasterRDD.metaData
    val tileRDD = rasterRDD map { case (key, tile) => key -> _op(tile) }
    new RasterRDD(tileRDD, metaData)
  }
}

case class LocalBinaryOp(
  op: LocalTileBinaryOp,
  opName: String,
  input: Seq[Node],
  constant: Option[Double] = None // Constant value,
) extends Node {
  def inputs = input
  def parameters = NodeParameters(constant=constant)
  val _op = op

  override def hashCode: Int = {
    41 * (
      41 * (
        41 + opName.hashCode
      ) + constant.hashCode
    ) + input.hashCode
  }


  def calc(zoom: Int, bounds: GridBounds) = {
    val metaData = input.head(zoom, bounds).metaData
    val tileRDD = input
      .map { _(zoom, bounds).tileRdd }
      .reduce { _ union _ }
      .reduceByKey(
        (aggr: Tile, value: Tile) => op(aggr, value)
      )

    val outTile =
      constant match {
        case Some(const) => {  // Have a constant
          metaData.cellType.isFloatingPoint match {  // Constant is floating point
            case true => tileRDD.map { case (key, tile) => key -> _op(tile, const.toDouble) }
            case false => tileRDD.map { case (key, tile) => key -> _op(tile, const) }
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
  def inputs = Seq(input)
  def parameters = NodeParameters(mappings=Some(mappings))
  val opName = this.getClass.getName

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

