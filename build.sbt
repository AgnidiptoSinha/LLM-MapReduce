ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.11"

lazy val root = (project in file("."))
  .settings(
    name := "untitled" ,
  )

libraryDependencies +="com.knuddels" % "jtokkit" % "1.1.0"
libraryDependencies +="org.tensorflow" % "tensorflow-core-platform" % "0.4.2"
libraryDependencies +="org.apache.hadoop" % "hadoop-common" % "3.3.4"
libraryDependencies +="org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.3.4"
libraryDependencies +="org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "3.3.4"