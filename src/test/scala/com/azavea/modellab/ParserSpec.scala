package com.azavea.modellab

import org.scalatest._
import geotrellis.spark._
import spray.json._
import DefaultJsonProtocol._
import geotrellis.raster.io.geotiff._
import geotrellis.raster._

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

class ParserSpec extends FunSpec with BeforeAndAfterAll {
  implicit val _sc = geotrellis.spark.utils.SparkUtils.createLocalSparkContext("local[*]", "Model Test")


  it("should evaluate basic AST") {
    
  }
  
  override def afterAll() { _sc.stop() }
}
