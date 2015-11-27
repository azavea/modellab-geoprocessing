package com.azavea.modellab.op

import com.azavea.modellab._
import geotrellis.raster._
import geotrellis.spark._

case class Coerce(input: Op) extends Op {
  def calc(zoom: Int, bounds: GridBounds) = {
    input(zoom, bounds).convert(TypeFloat)
  }

  def inputs = Seq(input)

  override def hashCode = "ConvertToFloat".hashCode

  override def toString = "ConvertToFloat"
}
