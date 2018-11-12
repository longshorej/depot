name := "depot"

organization := "com.github.longshorej"

version := sys.env.getOrElse("BUILD_VERSION", "1.0.0-SNAPSHOT")

description := "JVM bindings for depot"

publishMavenStyle := true

crossPaths := false

autoScalaLibrary := false

libraryDependencies ++= Seq(
  "junit"        % "junit"           % "4.12" % "test",
  "com.novocode" % "junit-interface" % "0.11" % "test"
)

fork := false
