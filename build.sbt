lazy val root = (project in file("."))
  .aggregate(core, kafka_0_8)
  .settings(noPublishSettings, commonSettings)

val sparkVersion = settingKey[String]("Spark version")
val sparkTestingVersion = settingKey[String]("Spark testing base version without Spark version part")

ThisBuild / libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
)

scalafixDependencies in ThisBuild +=
  "com.holdenkarau" %% "spark-scalafix-rules" % "0.1.1-2.4.8"

lazy val core = (project in file("core"))
  .settings(
    name := "spark-testing-base",
    commonSettings,
    publishSettings,
    coreSources,
    coreTestSources,
    addCompilerPlugin(scalafixSemanticdb),
     libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"        % sparkVersion.value,
      "org.apache.spark" %% "spark-streaming"   % sparkVersion.value,
      "org.apache.spark" %% "spark-sql"         % sparkVersion.value,
      "org.apache.spark" %% "spark-hive"        % sparkVersion.value,
      "org.apache.spark" %% "spark-catalyst"    % sparkVersion.value,
      "org.apache.spark" %% "spark-yarn"        % sparkVersion.value,
      "org.apache.spark" %% "spark-mllib"       % sparkVersion.value
    ) ++ commonDependencies ++
      {
        if (sparkVersion.value > "3.0.0") {
          Seq(
            "io.netty" % "netty-all" % "4.1.77.Final",
            "io.netty" % "netty-tcnative-classes" % "2.0.52.Final"
          )
        } else {
          // need a more recent version of xbean for Spark 2.4 so we support JDK11
          Seq(
            "org.apache.xbean" % "xbean-asm6-shaded" % "4.10",
          )
        }}

  )

lazy val kafka_0_8 = {
  Project("kafka_0_8", file("kafka-0.8"))
    .dependsOn(core)
    .settings(
      name := "spark-testing-kafka-0_8",
      commonSettings,
      kafkaPublishSettings,
      unmanagedSourceDirectories in Compile := {
        if (scalaVersion.value < "2.12.0")
          (unmanagedSourceDirectories in Compile).value
        else Seq.empty
      },
      unmanagedSourceDirectories in Test := {
        if (scalaVersion.value < "2.12.0")
          (unmanagedSourceDirectories in Test).value
        else Seq.empty
      },
      skip in compile := {
        scalaVersion.value >= "2.12.0"
      },
      skip in test := {
        scalaVersion.value >= "2.12.0"
      },
      skip in publish := {
        scalaVersion.value >= "2.12.0"
      },
      libraryDependencies ++= {
        excludeJpountz(
          if (scalaVersion.value >= "2.12.0") {
            Seq()
          } else {
            Seq(
              "org.apache.spark" %% "spark-streaming-kafka-0-8" % sparkVersion.value)
          }
        )
      }
    )
}

val commonSettings = Seq(
  organization := "com.holdenkarau",
  publishMavenStyle := true,
  sparkVersion := System.getProperty("sparkVersion", "2.4.8"),
  sparkTestingVersion := "1.3.4-SNAPSHOT",
  version := sparkVersion.value + "_" + sparkTestingVersion.value,
  scalaVersion := {
    "2.12.15"
  },
  crossScalaVersions := {
    if (sparkVersion.value >= "3.2.0") {
      Seq("2.12.15", "2.13.10")
    } else if (sparkVersion.value >= "3.0.0") {
      Seq("2.12.15")
    } else {
      Seq("2.12.15", "2.11.12")
    }
  },
  scalacOptions ++= Seq("-deprecation", "-unchecked", "-Yrangepos"),
  javacOptions ++= {
    Seq("-source", "1.8", "-target", "1.8")
  },
  javaOptions ++= Seq("-Xms5G", "-Xmx5G"),

  coverageHighlighting := true,

  parallelExecution in Test := false,
  fork := true,

  scalastyleSources in Compile ++= {unmanagedSourceDirectories in Compile}.value,
  scalastyleSources in Test ++= {unmanagedSourceDirectories in Test}.value,

  resolvers ++= Seq(
    "JBoss Repository" at "https://repository.jboss.org/nexus/content/repositories/releases/",
    "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
    "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
    "Twitter Maven Repo" at "https://maven.twttr.com/",
    "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
    "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
    "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/",
    "Second Typesafe repo" at "https://repo.typesafe.com/typesafe/maven-releases/",
    "Mesosphere Public Repository" at "https://downloads.mesosphere.io/maven",
    Resolver.sonatypeRepo("public")
  )
)

// Allow kafka (and other) utils to have version specific files
val coreSources = unmanagedSourceDirectories in Compile  := {
  if (sparkVersion.value >= "3.0.0" && scalaVersion.value >= "2.12.0") Seq(
    (sourceDirectory in Compile)(_ / "2.2/scala"),
    (sourceDirectory in Compile)(_ / "3.0/scala"),
    (sourceDirectory in Compile)(_ / "2.0/scala"), (sourceDirectory in Compile)(_ / "2.0/java")
  ).join.value
  else if (sparkVersion.value >= "2.4.0" && scalaVersion.value >= "2.12.0") Seq(
    (sourceDirectory in Compile)(_ / "2.2/scala"),
    (sourceDirectory in Compile)(_ / "2.0/scala"), (sourceDirectory in Compile)(_ / "2.0/java")
  ).join.value
  else Seq( // For scala 2.11 only bother building scala support, skip java bridge.
    (sourceDirectory in Compile)(_ / "2.2/scala"),
    (sourceDirectory in Compile)(_ / "2.0/scala")
  ).join.value
}

val coreTestSources = unmanagedSourceDirectories in Test  := {
  if (sparkVersion.value >= "3.0.0" && scalaVersion.value >= "2.12.0") Seq(
    (sourceDirectory in Test)(_ / "3.0/scala"),
    (sourceDirectory in Test)(_ / "3.0/java"),
    (sourceDirectory in Test)(_ / "2.2/scala"),
    (sourceDirectory in Test)(_ / "2.0/scala"),
    (sourceDirectory in Test)(_ / "2.0/java")
  ).join.value
  else if (sparkVersion.value >= "2.2.0" && scalaVersion.value >= "2.12.0") Seq(
    (sourceDirectory in Test)(_ / "2.2/scala"),
    (sourceDirectory in Test)(_ / "2.0/scala"), (sourceDirectory in Test)(_ / "2.0/java")
  ).join.value
  else Seq(
    (sourceDirectory in Test)(_ / "2.2/scala"),
    (sourceDirectory in Test)(_ / "2.0/scala")
  ).join.value
}



// additional libraries
lazy val commonDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.2.15",
  "org.scalatestplus" %% "scalacheck-1-15" % "3.2.3.0",
  "org.scalatestplus" %% "junit-4-12" % "3.2.2.0",
  "org.scalacheck" %% "scalacheck" % "1.15.2",
  "junit" % "junit" % "4.13.2",
  "org.eclipse.jetty" % "jetty-util" % "9.4.50.v20221201",
  "com.github.sbt" % "junit-interface" % "0.13.3" % "test->default")

// Based on Hadoop Mini Cluster tests from Alpine's PluginSDK (Apache licensed)
// javax.servlet signing issues can be tricky, we can just exclude the dep
def excludeFromAll(items: Seq[ModuleID], group: String, artifact: String) =
  items.map(_.exclude(group, artifact))

def excludeJavaxServlet(items: Seq[ModuleID]) =
  excludeFromAll(items, "javax.servlet", "servlet-api")

def excludeJpountz(items: Seq[ModuleID]) =
  excludeFromAll(items, "net.jpountz.lz4", "lz4")

lazy val kafkaPublishSettings =
  publishSettings ++ Seq(
    skip in publish := scalaVersion.value >= "2.12.0"
  )

// publish settings
lazy val publishSettings = Seq(
  pomIncludeRepository := { _ => false },
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases"  at nexus + "service/local/staging/deploy/maven2")
  },

  licenses := Seq("Apache License 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")),

  homepage := Some(url("https://github.com/holdenk/spark-testing-base")),

  scmInfo := Some(ScmInfo(
    url("https://github.com/holdenk/spark-testing-base.git"),
    "scm:git@github.com:holdenk/spark-testing-base.git"
  )),

  developers := List(
    Developer("holdenk", "Holden Karau", "holden@pigscanfly.ca", url("http://www.holdenkarau.com"))
  ),

  //credentials += Credentials(Path.userHome / ".ivy2" / ".spcredentials")
  credentials ++= Seq(Credentials(Path.userHome / ".ivy2" / ".sbtcredentials"), Credentials(Path.userHome / ".ivy2" / ".sparkcredentials")),
  useGpg := true,
  artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
    Artifact.artifactName(sv, module, artifact).replaceAll(s"-${module.revision}", s"-${sparkVersion.value}${module.revision}")
  }
)

lazy val noPublishSettings =
  skip in publish := true
