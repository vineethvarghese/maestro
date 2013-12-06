import AssemblyKeys._

name := "maestro"

version in ThisBuild := "0.1.1"

scalaVersion := "2.10.3"

scalacOptions := Seq(
  "-deprecation",
  "-unchecked",
  "-optimise",
  "-Ywarn-all",
  "-Xlint",
  "-Xfatal-warnings",
  "-feature",
  "-language:_"
)

assemblySettings

mainClass in assembly := Some("com.cba.maestro.main")
