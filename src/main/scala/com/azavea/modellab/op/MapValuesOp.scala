package com.azavea.modellab.op

import geotrellis.raster._
import geotrellis.spark._

case class MapValuesOp(
  input: Op,
  mappings: Seq[(Seq[Int], Option[Int])]
) extends Op {
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
