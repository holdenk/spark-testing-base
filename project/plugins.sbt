resolvers += Resolver.sonatypeRepo("releases")

import scala.util.Try

val sparkV: String = sys.props.getOrElse("sparkVersion", "2.4.8")

def major(v: String): Int = Try(v.takeWhile(_ != '.').toInt).getOrElse(2)

val scalafixPluginVersion: String =
  if (major(sparkV) >= 4) "0.14.2" else "0.11.1"


addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.3.1")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.3.1")

addDependencyTreePlugin

addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % scalafixPluginVersion)

ThisBuild / libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
)
