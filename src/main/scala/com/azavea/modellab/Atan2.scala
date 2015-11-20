package com.azavea.modellab

import geotrellis.raster._
import geotrellis.raster.op.local.LocalTileBinaryOp

object Atan2 extends LocalTileBinaryOp {
  def combine(z1: Int, z2: Int) =
    if (isNoData(z1) || isNoData(z2)) NODATA
    else math.atan2(z1, z2).toInt

  def combine(z1: Double, z2: Double) =
    if (isNoData(z1) || isNoData(z2)) Double.NaN
    else math.atan2(z1, z2)
}
