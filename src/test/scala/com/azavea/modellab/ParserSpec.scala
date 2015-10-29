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
  implicit val _sc = geotrellis.spark.utils.SparkUtils.createLocalSparkContext("local", "Model Test")

/*
  it("should evaluate basic AST"){
    import spray.json._
    import spray.json.DefaultJsonProtocol._

    val json = new String(Files.readAllBytes(Paths.get("sample_mask.json"))).parseJson;
    println(json)

    val parser = new Parser with DataHubCatalog {
      implicit val sc = _sc
    }
    
    val ast = parser.parse(json)
    info(ast.toString)
    
    val namedLayer = ast(11, GridBounds(594,774,596,776))
    info(namedLayer.values.first.asciiDraw)    
  }
 */
  
  override def afterAll() { _sc.stop() }
}
