name := "modellab"

Common.settings

resolvers += Resolver.bintrayRepo("azavea", "geotrellis")

libraryDependencies ++= Seq(
  Library.scalaTest,
  Library.logbackClassic,
  Library.sparkCore,
  Library.sprayHttpx, Library.sprayCan, Library.sprayRouting, Library.akka,
  Library.shapeless,
  Library.scalaz,
  Library.metrics, Library.metricsLibrato,
  Library.config,
  "io.spray"        %% "spray-json"    % "1.3.1",
  "com.azavea.geotrellis" %% "geotrellis-spark" % Version.geotrellis)

initialCommands in console :=
  """
  import geotrellis.raster._
  import geotrellis.vector._
  import geotrellis.proj4._
  import geotrellis.spark._
  import geotrellis.spark.utils._
  import geotrellis.spark.tiling._
  import shapeless._
  import syntax.singleton._ ; import record._
  import scalaz._
  import geotrellis.spark.utils.SparkUtils
  import com.azavea.modellab._
  val catalog = new DataHubCatalog {
    implicit val sc = geotrellis.spark.utils.SparkUtils.createLocalSparkContext("local[*]", "Model Service")
  }
  val parser = new Parser(new LayerRegistry, catalog.layerReader)
  """

