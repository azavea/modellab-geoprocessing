package com.azavea.modellab.op

import geotrellis.raster._
import geotrellis.raster.op.elevation._
import geotrellis.raster.op.focal._

case class FocalAspect(
  input: Op,
  n: Neighborhood
) extends Op {
  def calc(zoom: Int, bounds: GridBounds) = {
    val rasterRDD = input(zoom, bounds)
    val cs = rasterRDD.metaData.layout.rasterExtent.cellSize
    val _n = n
    rasterRDD.mapTiles { tile => Aspect(tile, _n, None, cs) }
  }

  def inputs = Seq(input)

  override def hashCode = ("FocalAspect", input, n).hashCode
  override def toString = s"FocalAspect(${n.getClass.getSimpleName}, $input)"
}
