import sbt._
import Keys._

import sbtassembly.Plugin._, AssemblyKeys._

object build extends Build {
  type Sett = Project.Setting[_]

  lazy val standardSettings: Seq[Sett] =
    Defaults.defaultSettings ++ Seq[Sett](
      version in ThisBuild := "0.1.1"
    , organization := "com.cba.omnia"
    , scalaVersion := "2.10.3"
    , scalacOptions := Seq(
        "-deprecation"
      , "-unchecked"
      , "-optimise"
// can't use -Ywarn-all, -Xlint because need to discard  -Ywarn-nullary-unit to work around scalding issue, resinstate once it is fixed.
//      , "-Ywarn-all"
//      , "-Xlint"
      , "-Ywarn-adapted-args"
      , "-Ywarn-dead-code"
      , "-Ywarn-inaccessible"
      , "-Ywarn-nullary-override"
      , "-Ywarn-numeric-widen"
      , "-Ywarn-value-discard"
      , "-Xfatal-warnings"
      , "-feature"
      , "-language:_"
      )
    , resolvers += Resolver.sonatypeRepo("releases")
    , resolvers += "commbank-releases" at "http://commbank.artifactoryonline.com/commbank/ext-releases-local"
    ) ++ UniqueVersion.uniqueVersionSettings

  lazy val all = Project(
    id = "all"
  , base = file(".")
  , settings = standardSettings ++ Seq[Sett](
      name := "maestro-all"
    , publishArtifact := false
    )
  , aggregate = Seq(core, macros, api, example, benchmark)
  ).dependsOn(api)

  lazy val api = Project(
    id = "api"
  , base = file("maestro-api")
  , settings = standardSettings ++ Seq[Sett](
      name := "maestro"
    , libraryDependencies ++= Seq(
      ) ++ tests ++ hadoop
    )
  ).dependsOn(core)
   .dependsOn(macros)

  lazy val core = Project(
    id = "core"
  , base = file("maestro-core")
  , settings = standardSettings ++ Seq[Sett](
      name := "maestro-core"
    , libraryDependencies ++= Seq(
        "com.google.guava"  %  "guava"       % "16.0.1"
      , "com.chuusai"       %% "shapeless"   % "2.0.0-M1" cross CrossVersion.full
      , "org.scalaz"        %% "scalaz-core" % "7.0.4"
      ) ++ tests ++ hadoop
    )
  ).dependsOn(file("../ebenezer"))

  lazy val macros = Project(
    id = "macros"
  , base = file("maestro-macros")
  , settings = standardSettings ++ Seq[Sett](
      name := "maestro-macros"
    , libraryDependencies <++= scalaVersion.apply(sv => Seq(
        "org.scala-lang" % "scala-compiler" % sv
      , "org.scala-lang" % "scala-reflect" % sv
      , "org.scalamacros" %% "quasiquotes" % "2.0.0-M3" cross CrossVersion.full
      ) ++ tests)
    , addCompilerPlugin("org.scalamacros" % "paradise" % "2.0.0-M3" cross CrossVersion.full)
    )
  ).dependsOn(core)

  lazy val example = Project(
    id = "example"
  , base = file("maestro-example")
  , settings = standardSettings ++ com.twitter.scrooge.ScroogeSBT.newSettings ++ assemblySettings ++ Seq[Sett](
      name := "maestro-example"
    , libraryDependencies ++= Seq(
      ) ++ tests ++ hadoop
    , mainClass in AssemblyKeys.assembly := None
    , test in assembly := {}
    , mergeStrategy in assembly <<= (mergeStrategy in assembly).apply(merge)
    )
  ).dependsOn(core)
   .dependsOn(macros)
   .dependsOn(api)

  lazy val benchmark = Project(
    id = "benchmark"
  , base = file("maestro-benchmark")
  , settings = standardSettings ++ com.twitter.scrooge.ScroogeSBT.newSettings ++  Seq[Sett](
      name := "maestro-benchmark"
    , libraryDependencies ++= Seq(
        "com.github.axel22" %% "scalameter" % "0.4"
      ) ++ tests
    , testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework")
    , parallelExecution in Test := false
    , logBuffered := false
    )
  ).dependsOn(core)
   .dependsOn(macros)
   .dependsOn(api)

  lazy val tests = Seq(
    "org.specs2"              %% "specs2"                     % "2.2.2"        % "test"
  , "org.scalacheck"          %% "scalacheck"                 % "1.10.1"       % "test"
  , "org.scalaz"              %% "scalaz-scalacheck-binding"  % "7.0.4"        % "test"
  )

  lazy val hadoop = Seq(
    "org.apache.hadoop"       %  "hadoop-client"      % "2.0.0-mr1-cdh4.3.0"  % "provided"
  , "org.apache.hadoop"       %  "hadoop-core"        % "2.0.0-mr1-cdh4.3.0"  % "provided"
  )

  def merge(old: String => MergeStrategy): String => MergeStrategy =
    x => {
      val oldstrat = old(x)
      if (oldstrat == MergeStrategy.deduplicate) MergeStrategy.first  else oldstrat
    }

}
