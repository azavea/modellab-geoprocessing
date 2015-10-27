resolvers ++= Seq(
  Classpaths.sbtPluginReleases,
  Opts.resolver.sonatypeReleases
)

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.13.0")
