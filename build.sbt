organization := "com.azavea.modellab"
name := "modellab-geoprocessing"
version := "0.1.0"

Common.settings

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
  """

libraryDependencies ++= Seq(
  Library.scalaTest,
  Library.logbackClassic,
  Library.sparkCore,
  Library.sprayHttpx, Library.sprayCan, Library.sprayRouting, Library.akka,
  Library.shapeless,
  Library.scalaz,
  "io.spray"        %% "spray-json"    % "1.3.1",
  "com.azavea.geotrellis" %% "geotrellis-spark" % Version.geotrellis)

resolvers += Resolver.bintrayRepo("azavea", "geotrellis")
resolvers += Resolver.bintrayRepo("scalaz", "releases")
resolvers += "OpenGeo" at "https://boundless.artifactoryonline.com/boundless/main"

test in assembly := {}

assemblyMergeStrategy in assembly := {
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case "META-INF/MANIFEST.MF" => MergeStrategy.discard
  case "META-INF\\MANIFEST.MF" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.SF" => MergeStrategy.discard
  case _ => MergeStrategy.first
}
