package com.azavea.modellab

import com.librato.metrics._
import java.util.concurrent._
import geotrellis.spark.utils.cache._
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.typesafe.config._

object Instrumented {
  /** The application wide metrics registry. */
  val metricRegistry = new com.codahale.metrics.MetricRegistry()

  {
    val config = ConfigFactory.load()
    config.hasPath("librato") {
      LibratoReporter.enable(
        LibratoReporter.builder(metricRegistry, 
          config.getString("librato.username"),
          config.getString("librato.token"),
          config.getString("librato.source"))
        5,
        TimeUnit.SECONDS);
    }
  }
}

trait Instrumented extends nl.grons.metrics.scala.InstrumentedBuilder {
  val logger = LoggerFactory.getLogger(this.getClass);
  val metricRegistry = Instrumented.metricRegistry
}