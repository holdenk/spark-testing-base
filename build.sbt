organization := "com.holdenkarau"

name := "spark-testing-base"

publishMavenStyle := true

version := "0.0.7-SNAPSHOT"

scalaVersion := "2.10.4"

crossScalaVersions := Seq("2.10.4", "2.11.6")

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

spName := "holdenk/spark-testing-base"

sparkVersion := "1.3.0"

sparkComponents ++= Seq("core", "streaming", "sql", "hive")

// additional libraries
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.1",
  "io.github.nicolasstucki" % "multisets_2.10" % "0.1",
  "org.scalacheck" %% "scalacheck" % "1.12.4" % "test",
  "junit" % "junit" % "4.12")

scalacOptions ++= Seq("-deprecation", "-unchecked")

pomIncludeRepository := { x => false }

resolvers ++= Seq(
  "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
  "Spray Repository" at "http://repo.spray.cc/",
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Twitter4J Repository" at "http://twitter4j.org/maven2/",
  "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
  "Twitter Maven Repo" at "http://maven.twttr.com/",
  "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
  "Mesosphere Public Repository" at "http://downloads.mesosphere.io/maven",
  Resolver.sonatypeRepo("public")
)

// publish settings
publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

licenses := Seq("Apache License 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))

homepage := Some(url("https://github.com/holdenk/spark-testing-base"))

pomExtra := (
  <scm>
    <url>git@github.com:holdenk/spark-testing-base.git</url>
    <connection>scm:git@github.com:holdenk/spark-testing-base.git</connection>
  </scm>
  <developers>
    <developer>
      <id>holdenk</id>
      <name>Holden Karau</name>
      <url>http://www.holdenkarau.com</url>
      <email>holden@pigscanfly.ca</email>
    </developer>
  </developers>
)

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")
