package com.azavea.modellab

import geotrellis.raster._
import geotrellis.spark.SpatialKey
import geotrellis.vector._
import geotrellis.vector.reproject._
import geotrellis.proj4._

import geotrellis.spark.tiling._

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Help to sample neighborhood on one of the rendered layers
 */
class SampleNeighborhood(getTile: (String, Int, Int, Int) => Option[Future[Option[Tile]]]) {
  val tmsScheme = ZoomedLayoutScheme(LatLng, 256)

  //TODO: this should be in geotrellis.raster
  def stitch(tileSeq: Seq[(SpatialKey, Tile)], layout: LayoutDefinition): Raster = {
    val tileMap = tileSeq.toMap
    val cellType = tileSeq.head._2.cellType

    val tileCols = layout.tileLayout.tileCols
    val tileRows = layout.tileLayout.tileRows

    // discover what I have here, in reality RasterMetaData should reflect this already
    val te = GridBounds.envelope(tileMap.keys)
    val tileExtent: Extent = layout.mapTransform(te)

    val tiles = te.coords map { case (col, row) =>
      tileMap.getOrElse(col -> row, EmptyTile(cellType, tileCols, tileRows))
    }

    Raster(CompositeTile(tiles, TileLayout(te.width, te.height, tileCols, tileRows)), tileExtent)
  }

  // LatLng -> Extent(via CellSize) -> GridBounds(via MapKeyTransform) -> Raster(stitch on collection) -> crop by Extent
  def sample(name: String, zoom: Int, lng: Double, lat: Double, cellBuffer: Int): Future[Tile] = {
    val wmPoint = Point(lng, lat).reproject(LatLng, WebMercator)
    val layout = tmsScheme.levelForZoom(WebMercator.worldExtent, zoom).layout
    val sampleExtent = wmPoint.buffer(cellBuffer * layout.rasterExtent.cellheight).envelope
    val gridBounds: GridBounds = layout.mapTransform(sampleExtent)

    val futureTiles: Seq[Future[Option[(SpatialKey, Tile)]]] = {
      for {(col, row) <- gridBounds.coords} yield
      getTile(name, zoom, col, row).map(o => o.map(fot => fot.map(ot => SpatialKey(col, row) -> ot)))
    }.flatten.toSeq

    val futureMaybeTiles: Future[Seq[Option[(SpatialKey, Tile)]]] = Future.sequence(futureTiles)

    futureMaybeTiles.map { aot =>
      val tiles = aot.flatten
      val raster: Raster = stitch(tiles, layout)
      raster.crop(sampleExtent).tile
    }
  }
}
