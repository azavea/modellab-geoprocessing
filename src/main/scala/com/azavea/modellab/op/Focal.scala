package com.azavea.modellab.op

import geotrellis.raster._
import geotrellis.raster.op.focal._
import geotrellis.spark._
import geotrellis.spark.op.focal._

case class Focal(
  name: String,
  op: (Tile, Neighborhood, Option[GridBounds]) => Tile,
  input: Op,
  n: Neighborhood
) extends Op {
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
