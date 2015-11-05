package com.azavea.modellab.op

import geotrellis.raster._
import geotrellis.raster.op.elevation._
import geotrellis.raster.op.focal._

case class AspectOp(
  input: Op,
  n: Neighborhood
) extends Op {
  def calc(zoom: Int, bounds: GridBounds) = {
    val rasterRDD = input(zoom, bounds)
    val cs = rasterRDD.metaData.layout.rasterExtent.cellSize
    rasterRDD.mapTiles { tile => Aspect(tile, n, None, cs) }
  }

  def inputs = Seq(input)

  override def hashCode = ("Aspect", input, n).hashCode
  override def toString = s"AspectOp(${n.getClass.getSimpleName}, $input)"
}
