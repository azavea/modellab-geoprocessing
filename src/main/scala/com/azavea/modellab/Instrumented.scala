package com.azavea.modellab

import com.librato.metrics._
import java.util.concurrent._
import geotrellis.spark.utils.cache._
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

object Instrumented {
  /** The application wide metrics registry. */
  val metricRegistry = new com.codahale.metrics.MetricRegistry()

  LibratoReporter.enable(
    LibratoReporter.builder(
        metricRegistry,
        "echeipesh@gmail.com",
        "60d8dd9074d0e554a643334a23800b0bfae6ebcd210377e7f6dfa99f29dc8b41",
        "bear"),
    5,
    TimeUnit.SECONDS);
}

trait Instrumented extends nl.grons.metrics.scala.InstrumentedBuilder {
  val logger = LoggerFactory.getLogger(this.getClass);
  val metricRegistry = Instrumented.metricRegistry
}