import sbt._
import sbt.Keys._

object Common {
  val settings =
    List(
      organization := "com.azavea",
      version := "1.0.0",
      scalaVersion := Version.scala,
      crossScalaVersions := List(scalaVersion.value),
      scalacOptions ++= List(
        "-unchecked",
        "-deprecation",
        "-language:_",
        "-target:jvm-1.6",
        "-encoding", "UTF-8"
      )
    )
}
