package com.azavea.modellab

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.raster._
import geotrellis.raster.op.local._

import java.lang.ref.WeakReference
import scala.collection.mutable.Stack
import scala.collection.concurrent.TrieMap

object RPN extends DataHubCatalog {
  implicit val sc = geotrellis.spark.utils.SparkUtils.createLocalSparkContext("local[*]", "ModelLab Service")

  val cache = TrieMap.empty[(String, Int), WeakReference[GridBounds => RasterRDD[SpatialKey]]]
  val terminalCache = TrieMap.empty[(Char, Int), WeakReference[GridBounds => RasterRDD[SpatialKey]]]

  val terminals = ('a' to 'z')
  val nonTerminals = ('A' to 'Z')
  val digits = ('0' to '9')

  /**
    * Convert a terminal to a lambda which returns an RDD when a
    * GridBounds is applied to it.
    */
  def terminalToLambda(t : Char, zoom : Int) : GridBounds => RasterRDD[SpatialKey] = {
    val layerName = t match {
      case 'a' => "nlcd-zoomed"
      case _ => "nlcd-zoomed"
    }

    println(s"terminal: $t")

    // function from GridBounds to RDD
    bounds : GridBounds => layerReader.query(LayerId(layerName, zoom)).where(Intersects(bounds)).toRDD
  }

  /**
    * Convert a valid subtree to a lambda which returns an RDD when a
    * GridBounds is applied to it.
    */
  def subtreeToLambda(nt : Char,
    _left : GridBounds => RasterRDD[SpatialKey],
    _right : GridBounds => RasterRDD[SpatialKey]
  ) : GridBounds => RasterRDD[SpatialKey] = {
    val op = nt match {
      case 'A' => Add
      case 'B' => Subtract
      case 'C' => Multiply
      case 'D' => Divide
      case 'E' => And
      case 'F' => Or
      case 'G' => Xor
      case 'H' => Min
      case 'I' => Max
      case 'J' => Pow
      case _ => Add
    }

    println(s"non-terminal: $nt")

    // function from GridBounds to RDD
    bounds : GridBounds => {
      val left = _left(bounds)
      val right = _right(bounds)
      left.combineTiles(right) { op(_,_) }
    }
  }

  /**
    * Parse RPN-formatted request.
    */
  def parse(str : String, zoom : Int) : GridBounds => RasterRDD[SpatialKey] = {
    val stack = Stack.empty[GridBounds => RasterRDD[SpatialKey]]
    val indexStack = Stack.empty[Int]

    for (pair <- str zip (0 to str.length)) {
      val sym = pair._1
      val idx = pair._2

      // Handle terminals (pre-defined layers)
      if (terminals contains sym) {
        val key = (sym, zoom)
        val r = terminalCache.getOrElseUpdate(key, new WeakReference(terminalToLambda(sym, zoom)))
        r.get match {
          case null => stack.push(terminalToLambda(sym, zoom)) // "This should never happen"
          case fn => stack.push(fn)
        }
        indexStack.push(idx)
      }
      // Handle non-terminals (raster operations)
      else if (nonTerminals contains sym) {
        val left = stack.pop()
        val right = stack.pop()
        val leftIdx = indexStack.pop()
        val rightIdx = indexStack.pop()
        val subexp = str.substring(Math.min(leftIdx, rightIdx), idx + 1)
        val key = (subexp, zoom)
        val r = cache.getOrElseUpdate(key, new WeakReference(subtreeToLambda(sym, left, right)))
        r.get match {
          case null => stack.push(subtreeToLambda(sym, left, right)) // "This should never happen"
          case fn => stack.push(fn)
        }
        indexStack.push(idx)
      }
    }

    stack.pop()
  }
}
