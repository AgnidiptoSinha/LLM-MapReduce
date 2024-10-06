ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.11"

lazy val root = (project in file("."))
  .settings(
    name := "untitled",
    libraryDependencies ++= Seq(
      "com.knuddels" % "jtokkit" % "1.1.0",
      "org.tensorflow" % "tensorflow-core-platform" % "0.4.2",
      "org.apache.hadoop" % "hadoop-common" % "3.3.4",
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.3.4",
      "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "3.3.4",
      "org.scalanlp" %% "breeze" % "2.1.0",
      "org.scalanlp" %% "breeze-natives" % "2.1.0",
      "org.slf4j" % "slf4j-api" % "1.7.32",
      "ch.qos.logback" % "logback-classic" % "1.2.6",
      "com.typesafe" % "config" % "1.4.2",
      "org.scalatest" %% "scalatest" % "3.2.15" % Test
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    }
  )

// Add this line to exclude Scala library from the assembled JAR
//assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false)