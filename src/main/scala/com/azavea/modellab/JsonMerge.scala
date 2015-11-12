package com.azavea.modellab

import spray.json._

import scala.util.Try

object JsonMerge {
  /** merge two maps, source values preferentially from left map */
  def mergeMaps(m1: Map[String, JsValue], m2: Map[String, JsValue]): Map[String, JsValue] = {
    for (key <- m1.keySet ++ m2.keySet) yield {
      val value: JsValue = m1.get(key) match {
        case Some(left: JsObject) =>
          m2.get(key) match {
            case Some(right) => JsonMerge(left, right)
            case None => left
          }
        case Some(left: JsArray) =>
          m2.get(key) match {
            case Some(right: JsArray) =>
              JsArray(for ( (l, r) <- left.elements zip right.elements )
                yield JsonMerge(l, r))
            case _ => left
          }
        case Some(j: JsValue) =>
          j
        case None =>
          m2(key)
      }
      key -> value
    }
  }.toMap

  /** Merge two Json trees, recursion through object, preferring values on lefts side in conflict. */
  def apply(o1: JsValue, o2: JsValue): JsValue = {
    Try{
      JsObject(mergeMaps(o1.asJsObject.fields, o2.asJsObject.fields))
    }.getOrElse(o1)
  }
}
