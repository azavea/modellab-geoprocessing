package com.azavea.modellab.op

import geotrellis.raster._
import geotrellis.raster.op.local._
import geotrellis.spark._

case class LocalBinaryOp(
  op: LocalTileBinaryOp,
  inputs: Seq[Op],
  const: Option[Double] = None // Constant value
) extends Op {
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
