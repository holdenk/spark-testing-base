resolvers += Resolver.sonatypeRepo("releases")

import scala.util.Try

val sparkV: String = sys.props.getOrElse("sparkVersion", "3.0.2")

def major(v: String): Int = Try(v.takeWhile(_ != '.').toInt).getOrElse(3)


addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.3.1")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.3.1")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.4.0")

addDependencyTreePlugin

// Only enable ScalaFix for Spark 4 because of Scala version issues.
// You _could_ use this in Spark 3 but not with 2.12/2.13 cross compile.
val maybeScalafix = if (major(sparkV) == 4) {
  Seq(addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.14.2"))
} else {
  Seq.empty
}

maybeScalafix

ThisBuild / libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
)
