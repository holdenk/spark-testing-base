resolvers += Resolver.sonatypeRepo("releases")

import scala.util.Try

val sparkV: String = sys.props.getOrElse("sparkVersion", "3.5.7")

def major(v: String): Int = Try(v.takeWhile(_ != '.').toInt).getOrElse(2)

def minor(v: String): Int = {
  val afterFirstDot = v.dropWhile(_ != '.').drop(1)
  Try(afterFirstDot.takeWhile(_.isDigit).toInt).getOrElse(0)
}

val scalafixPluginVersion: String =
  if (major(sparkV) == 2) "0.11.1" else "0.14.2"


addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.3.1")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.4.0")

addDependencyTreePlugin

// Only enable ScalaFix for Spark 2.X & Spark 4 because of Scala version issues.
// You _could_ use this in Spark 3 but not with 2.12/2.13 cross compile.
val maybeScalafix = if (major(sparkV) == 4 || major(sparkV) == 2) {
  Seq(addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % scalafixPluginVersion))
} else {
  Seq.empty
}

maybeScalafix

ThisBuild / libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
)
