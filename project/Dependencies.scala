import sbt._

object Version {
  val geotrellis   = "0.10.0-83483fc"
  val logback      = "1.1.2"
  val scala        = "2.10.6"
  val scalaTest    = "2.2.0"
  val spark        = "1.5.1"
  val spray        = "1.3.2"
  val akka         = "2.3.9"
  val scalaz       = "7.1.4"
  val shapeless    = "2.1.0"
}

object Library {
  val sprayRouting  = "io.spray"        %% "spray-routing-shapeless2" % Version.spray
  val sprayTestkit  = "io.spray"        %% "spray-testkit" % Version.spray
  val sprayCan      = "io.spray"        %% "spray-can"     % Version.spray
  val sprayHttpx    = "io.spray"        %% "spray-httpx"   % Version.spray
  val akka            =  "com.typesafe.akka"     %%  "akka-actor"                   % Version.akka
  val shapeless       = "com.chuusai"            % "shapeless_2.10.4"               % Version.shapeless
  val scalaz          = "org.scalaz"             %% "scalaz-core"                   % Version.scalaz
  val logbackClassic  = "ch.qos.logback"         %  "logback-classic"               % Version.logback
  val sparkCore       = "org.apache.spark"       %% "spark-core"                    % Version.spark
  val scalaTest       = "org.scalatest"          %% "scalatest"                     % Version.scalaTest
  val metrics         = "nl.grons"               %% "metrics-scala"                 % "3.5.2_a2.3"
  val metricsLibrato  = "com.librato.metrics"    % "metrics-librato"                % "4.0.1.12"
  val config          = "com.typesafe"           % "config"                         % "1.2.1"
}