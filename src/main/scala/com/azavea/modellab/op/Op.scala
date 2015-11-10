package com.azavea.modellab.op

import java.nio.ByteBuffer

import com.azavea.modellab._
import geotrellis.raster._
import geotrellis.spark._
import org.apache.commons.codec.binary._

import scala.collection.concurrent.TrieMap

object Op {
  type K = (Op, Int, GridBounds)
  type V = RasterRDD[SpatialKey]
  val cache = new TrieMap[K,V]
}

trait Op extends Serializable with Instrumented {
  private[this] val cacheHit = metrics.counter("cache.hit")
  private[this] val cacheMiss = metrics.counter("cache.miss")

  def calc(zoom: Int, bounds: GridBounds): RasterRDD[SpatialKey]

  def apply(zoom: Int, bounds: GridBounds): RasterRDD[SpatialKey] = {
    // This will backstop calculation of RDDs nodes, hopefully allowing IO nodes to be re-used
    // Critical note: the Node tree will already exist fully.
    val key = (this, zoom, bounds)
    val name = s"${this.getClass.getSimpleName}($zoom, $bounds)"

    // NOTE: We used to cache here to store every layer and their intermidiate results
    // We actually disable the cache because it appears caching result tiles in LRU cache on the driver is more efficient
    //  likely there will be cases where that is not true, benchmarking may change this conclusion
    
    // Node.cache.synchronized {
    //   if ( Node.cache.contains(key) ) cacheHit += 1 else cacheMiss += 1      
    //   Node.cache.getOrElseUpdate(key, calc(zoom, bounds).setName(name).cache)
    // }
    calc(zoom, bounds).setName(name)
  }

  def inputs: Seq[Op]

  def hashString: String = {
    val buff = ByteBuffer.allocate(4)
    buff.putInt(hashCode)
    //32 bit int in base32 is always going to be padded with '='
    (new Base32).encode(buff.array).map(_.toChar).mkString.substring(0,7)
  }
}