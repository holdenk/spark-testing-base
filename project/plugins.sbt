resolvers += Resolver.sonatypeRepo("releases")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.1.2")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.7")

addDependencyTreePlugin

addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.10.4")

ThisBuild / libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
)
