package com.azavea.modellab.op

import geotrellis.raster._
import geotrellis.spark._

case class MapRanges(
  input: Op,
  mappings: Seq[((Double, Double), Option[Double])]
) extends Op {
  def calc(zoom: Int, bounds: GridBounds) = {
    val rasterRDD = input(zoom, bounds)
    val metaData = rasterRDD.metaData
    // Construct a list of partial functions to be chained together
    val partialFuncs: Seq[PartialFunction[Double, Double]] =
      mappings map { case (range: (Double, Double), mapped: Option[Double]) =>
        // Partial functions require explicit typing
        val partFunc: PartialFunction[Double, Double] = mapped match {
          case Some(number) => { case (dub: Double) =>
            if (dub > range._1 && dub < range._2) number else dub }
          case None => { case (dub: Double) =>
            Double.NaN }
        }
        partFunc
      }
    // A fallback partial function for use on cells that don't fall into specified ranges
    val noop: PartialFunction[Double, Double] = { case (x: Double) => x }
    // Chain it all together (reduceLeft because order is important)
    val rangeMap = partialFuncs.reduceLeft(_ orElse _) orElse noop

    val tileRDD = rasterRDD map { case (key, tile) =>
      key -> tile.mapDouble { cellValue => rangeMap(cellValue) }
    }
    new RasterRDD(tileRDD, metaData)
  }

  def inputs = Seq(input)

  override def hashCode = (input, mappings).hashCode
}
