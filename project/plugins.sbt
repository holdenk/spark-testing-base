resolvers += Resolver.sonatypeRepo("releases")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.1.1")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.1")

addDependencyTreePlugin

addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.10.4")

ThisBuild / libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
)
