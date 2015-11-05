package com.azavea.modellab.op

import geotrellis.raster._
import geotrellis.raster.op.elevation._
import geotrellis.raster.op.focal._

case class SlopeOp(
  input: Op,
  n: Neighborhood,
  z: Double
) extends Op {
  def calc(zoom: Int, bounds: GridBounds) = {
    val rasterRDD = input(zoom, bounds)
    val cs = rasterRDD.metaData.layout.rasterExtent.cellSize
    rasterRDD.mapTiles { tile => Slope(tile, n, None, cs, z) }
  }

  def inputs = Seq(input)

  override def hashCode = ("Slope", input, n, z).hashCode
  override def toString = s"SlopeOp(${n.getClass.getSimpleName}, $input)"
}
