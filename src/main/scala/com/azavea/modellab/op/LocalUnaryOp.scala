package com.azavea.modellab.op

import geotrellis.raster._

case class LocalUnaryOp(
  name: String,
  op: Tile => Tile,
  input: Op
) extends Op {
  def calc(zoom: Int, bounds: GridBounds) = {
    val _op = op
    input(zoom, bounds).mapTiles(_op)
  }

  def inputs = Seq(input)

  override def hashCode = name.hashCode + input.hashCode
  override def toString = s"LocalUnaryOp($name, $input)"
}
