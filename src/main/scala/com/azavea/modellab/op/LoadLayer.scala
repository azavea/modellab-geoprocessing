package com.azavea.modellab.op

import com.azavea.modellab._
import geotrellis.raster._
import geotrellis.spark._

case class LoadLayer(layerName: String, layerReader: WindowedReader) extends Op {
  def calc(zoom: Int, bounds: GridBounds) = {
    layerReader.getView(LayerId(layerName, zoom), bounds)
  }

  def inputs = Seq.empty

  override def hashCode = layerName.hashCode

  override def toString = s"LoadLayer($layerName)"
}
