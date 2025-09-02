lazy val root = (project in file("."))
  .aggregate(core, kafka_0_8)
  .settings(noPublishSettings, commonSettings)

//tag::sparkVersion[]
val sparkVersion = settingKey[String]("Spark version")
//end::sparkVersion[]
val sparkTestingVersion = settingKey[String]("Spark testing base version without Spark version part")

def specialOptions = {
  // We only need these extra props for JRE>17
  if (sys.props("java.specification.version") > "1.17") {
    Seq(
      "base/java.lang", "base/java.lang.invoke", "base/java.lang.reflect", "base/java.io", "base/java.net", "base/java.nio",
      "base/java.util", "base/java.util.concurrent", "base/java.util.concurrent.atomic",
      "base/sun.nio.ch", "base/sun.nio.cs", "base/sun.security.action",
      "base/sun.util.calendar", "security.jgss/sun.security.krb5",
    ).map("--add-opens=java." + _ + "=ALL-UNNAMED")
  } else {
    Seq()
  }
}


ThisBuild / libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
)

// Disabled for now because we are not upgrading & it doesn't play nice with Scala 2.12 & 2.13 at the same time.
//scalafixDependencies in ThisBuild +=
//  "com.holdenkarau" %% "spark-scalafix-rules" % "0.1.1-2.4.8"

lazy val core = (project in file("core"))
  .settings(
    name := "spark-testing-base",
    commonSettings,
    publishSettings,
    coreSources,
    coreTestSources,
// Disabled for now because we are not upgrading & it doesn't play nice with Scala 2.12 & 2.13 at the same time.
//    addCompilerPlugin(scalafixSemanticdb),
     libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"        % sparkVersion.value,
      "org.apache.spark" %% "spark-streaming"   % sparkVersion.value,
      "org.apache.spark" %% "spark-sql"         % sparkVersion.value,
      "org.apache.spark" %% "spark-hive"        % sparkVersion.value,
      "org.apache.spark" %% "spark-catalyst"    % sparkVersion.value,
      "org.apache.spark" %% "spark-mllib"       % sparkVersion.value
    ) ++ commonDependencies ++
      {
        if (sparkVersion.value > "4.0.0") {
          Seq(
            "org.apache.spark" %% "spark-sql-api"        % sparkVersion.value,
            "io.netty" % "netty-all" % "4.1.96.Final",
            "io.netty" % "netty-tcnative-classes" % "2.0.66.Final",
            "com.github.luben" % "zstd-jni" % "1.5.5-4"
          )
        } else if (sparkVersion.value > "3.0.0") {
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
  organizationName := "Holden Karau",
  organizationHomepage := Some(url("http://www.holdenkarau.com")),
  publishMavenStyle := true,
  libraryDependencySchemes += "com.github.luben" %% "zstd-jni" % "early-semver", // "early-semver",
  evictionErrorLevel := Level.Info,
  sparkVersion := System.getProperty("sparkVersion", "2.4.8"),
  isSnapshot := true,
  sparkTestingVersion := "2.1.2-SNAPSHOT",
  version := sparkVersion.value + "_" + sparkTestingVersion.value,
  scalaVersion := {
    if (sparkVersion.value >= "4.0.0") {
      "2.13.16"
    } else {
      "2.12.15"
    }
  },
  //tag::dynamicScalaVersion[]
  crossScalaVersions := {
    if (sparkVersion.value >= "4.0.0") {
      Seq("2.13.16") // Minor version incompat will break, ah Scala :p
    } else if (sparkVersion.value >= "3.2.0") {
      Seq("2.12.15", "2.13.16")
    } else if (sparkVersion.value >= "3.0.0") {
      Seq("2.12.15")
    } else {
      Seq("2.12.15", "2.11.12")
    }
  },
  //end::dynamicScalaVersion[]
  scalacOptions ++= Seq("-deprecation", "-unchecked", "-Yrangepos"),
  javacOptions ++= {
    if (sparkVersion.value >= "4.0.0") {
      Seq("-source", "17", "-target", "17")
    } else {
      Seq("-source", "1.8", "-target", "1.8")
    }
  },
  javaOptions ++= Seq("-Xms8G", "-Xmx8G"),

  coverageHighlighting := true,

  Test / javaOptions ++= specialOptions,

  parallelExecution in Test := false,
  fork := true,

  scalastyleSources in Compile ++= {unmanagedSourceDirectories in Compile}.value,
  scalastyleSources in Test ++= {unmanagedSourceDirectories in Test}.value,

  resolvers ++= (Seq(
    Resolver.typesafeRepo("releases"),
    Resolver.sbtPluginRepo("releases"),
    Resolver.sonatypeRepo("public"),
    Resolver.mavenLocal,
    Resolver.DefaultMavenRepository,
  ) ++ Resolver.sonatypeOssRepos("releases") ++
    Resolver.sonatypeOssRepos("snapshots"))

)

// Allow kafka (and other) utils to have version specific files
//tag::dynamicCodeSelection[]
val coreSources = unmanagedSourceDirectories in Compile  := {
  if (sparkVersion.value >= "4.0.0") Seq(
    (sourceDirectory in Compile)(_ / "4.0/scala"),
    (sourceDirectory in Compile)(_ / "2.2/scala"),
    (sourceDirectory in Compile)(_ / "3.0/scala"),
    (sourceDirectory in Compile)(_ / "2.0/scala"),
    (sourceDirectory in Compile)(_ / "2.0/java")
  ).join.value
  else if (sparkVersion.value >= "3.0.0" && scalaVersion.value >= "2.12.0") Seq(
    (sourceDirectory in Compile)(_ / "2.2/scala"),
    (sourceDirectory in Compile)(_ / "3.0/scala"),
    (sourceDirectory in Compile)(_ / "2.0/scala"),
    (sourceDirectory in Compile)(_ / "2.0/java")
  ).join.value
//end::dynamicCodeSelection[]
  else if (sparkVersion.value >= "3.0.0" && scalaVersion.value >= "2.12.0") Seq(
    (sourceDirectory in Compile)(_ / "2.2/scala"),
    (sourceDirectory in Compile)(_ / "3.0/scala"),
    (sourceDirectory in Compile)(_ / "2.0/scala"),
    (sourceDirectory in Compile)(_ / "2.0/java")
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
  if (sparkVersion.value >= "4.0.0" && scalaVersion.value >= "2.12.0") Seq(
    (sourceDirectory in Test)(_ / "4.0/scala"),
    (sourceDirectory in Test)(_ / "3.0/scala"),
    (sourceDirectory in Test)(_ / "3.0/java"),
    (sourceDirectory in Test)(_ / "2.2/scala"),
    (sourceDirectory in Test)(_ / "2.0/scala"),
    (sourceDirectory in Test)(_ / "2.0/java")
  ).join.value
  else if (sparkVersion.value >= "3.0.0" && scalaVersion.value >= "2.12.0") Seq(
    (sourceDirectory in Test)(_ / "pre-4.0/scala"),
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
  "org.scalatest" %% "scalatest" % "3.2.17",
  "org.scalatestplus" %% "scalacheck-1-15" % "3.2.3.0",
  "org.scalatestplus" %% "junit-4-13" % "3.2.19.1",
  "org.scalacheck" %% "scalacheck" % "1.15.2",
  "junit" % "junit" % "4.13.2",
  "org.eclipse.jetty" % "jetty-util" % "9.4.51.v20230217",
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

val centralSnapshots = "https://ossrh-staging-api.central.sonatype.com/service/local/staging/deploy/maven2/"

// publish settings
lazy val publishSettings = Seq(
  pomIncludeRepository := { _ => false },
  stagingDirectory := (ThisBuild / baseDirectory).value / "target" / "sona-staging-${name.value}-${version.value}",

  publishTo := {
    if (isSnapshot.value) Some("central-snapshots" at centralSnapshots)
    else localStaging.value
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

  credentials ++= Seq(
    ".sbtcredentials",
    ".sbt_staging_credentials",
    ".sparkcredentials").map(
    n => Path.userHome / ".ivy2" / n)
  .filter(_.exists)
  .map(Credentials(_)),
  useGpg := true,
  artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
    Artifact.artifactName(sv, module, artifact).replaceAll(s"-${module.revision}", s"-${sparkVersion.value}${module.revision}")
  }
)

lazy val noPublishSettings = {
  skip in publish := true
}
