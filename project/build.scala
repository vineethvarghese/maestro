import sbt._
import Keys._

import sbtassembly.Plugin.AssemblyKeys._

import au.com.cba.omnia.uniform.core.standard.StandardProjectPlugin._
import au.com.cba.omnia.uniform.core.version.UniqueVersionPlugin._
import au.com.cba.omnia.uniform.dependency.UniformDependencyPlugin._
import au.com.cba.omnia.uniform.thrift.UniformThriftPlugin._
import au.com.cba.omnia.uniform.assembly.UniformAssemblyPlugin._

import sbtassembly.Plugin._, AssemblyKeys._

object build extends Build {
  type Sett = Project.Setting[_]

  val IncludeInLib = Configurations.config("include-lib").extend(Compile)

  def addSpecifiedDependenciesToLibInAssembly = inConfig(IncludeInLib)(Defaults.configSettings) ++ Seq(
    (libraryDependencies in IncludeInLib) := Nil, // needed otherwise scala library gets included.
    (assembledMappings in assembly) <<= (managedClasspath in IncludeInLib, assembledMappings in assembly) map ((managed, mappings) => {
      {
        val managedMappings = managed.map(_.data).map(f => f -> ("lib/" + f.getName))
        managedMappings.foreach({ case (file, path) => println("Adding managed (include in lib) dependency at " + path)})
 managedMappings ++ mappings
      }
    })
  )

  lazy val standardSettings: Seq[Sett] =
    Defaults.defaultSettings ++ Seq[Sett](
      version in ThisBuild := "0.1.1"
     ,scalaVersion := "2.10.3"
     ,scalacOptions := Seq(                                                                                                                                                                                                                                                       
      "-deprecation"                                                                                                                                                                                                                                                             
     ,"-unchecked"
     , "-Ywarn-all"
     , "-Xlint"
     , "-feature"
     , "-language:_"
     , "-target:jvm-1.6"
     )
    ) ++ uniqueVersionSettings ++ uniformDependencySettings

  lazy val all = Project(
    id = "all"
  , base = file(".")
  , settings = standardSettings ++ uniform.project("maestro-all", "au.com.cba.omnia.maestro") ++ Seq[Sett](
      publishArtifact := false
    )
  , aggregate = Seq(core, macros, api, example)
  )

  lazy val api = Project(
    id = "api"
  , base = file("maestro-api")
  , settings =
       standardSettings
    ++ uniform.project("maestro", "au.com.cba.omnia.maestro.api")
    ++ Seq[Sett](
      libraryDependencies ++= depend.hadoop() ++ depend.testing() 
    )
  ).dependsOn(core)
   .dependsOn(macros)

  lazy val core = Project(
    id = "core"
  , base = file("maestro-core")
  , settings =
       standardSettings
    ++ uniform.project("maestro-core", "au.com.cba.omnia.maestro.core")
    ++ Seq[Sett](
      libraryDependencies ++= Seq(
        "com.chuusai"       %% "shapeless"       % "2.0.0-M1" cross CrossVersion.full
      , "com.google.guava"  %  "guava"           % "16.0.1"
      , "cascading"         %  "cascading-hive"  % "1.0.0-wip-dev"
      ) ++ depend.scalaz() ++ depend.omnia("ebenezer", "0.0.1-20140317103613-682d854") ++ depend.scalding() ++ depend.hadoop() ++ depend.testing()
    )
  )

  lazy val macros = Project(
    id = "macros"
  , base = file("maestro-macros")
  , settings =
       standardSettings
    ++ uniform.project("maestro-macros", "au.com.cba.omnia.maestro.macros")
    ++ Seq[Sett](
      libraryDependencies <++= scalaVersion.apply(sv => Seq(
        "org.scala-lang" % "scala-compiler" % sv
      , "org.scala-lang" % "scala-reflect" % sv
      , "org.scalamacros" %% "quasiquotes" % "2.0.0-M3" cross CrossVersion.full
      ) ++ depend.testing())
    , addCompilerPlugin("org.scalamacros" % "paradise" % "2.0.0-M3" cross CrossVersion.full)
    )
  ).dependsOn(core)

  lazy val example = Project(
    id = "example"
  , base = file("maestro-example")
  , settings =
    standardSettings ++
    uniform.project("maestro-example", "au.com.cba.omnia.maestro.example") ++
    (uniformAssemblySettings: Seq[Sett]) ++
    (uniformThriftSettings: Seq[Sett]) ++
    Seq[Sett](
     libraryDependencies ++= depend.hadoop() ++ depend.testing()
    , mergeStrategy in assembly <<= (mergeStrategy in assembly)(fixLicenses)
    , addSpecifiedDependenciesToLibInAssembly
    , (libraryDependencies in IncludeInLib) := Seq("org.apache.hive" % "hive-builtins" % "0.10.0.jar" intransitive())
    )
  ).dependsOn(core)
   .dependsOn(macros)
   .dependsOn(api)

  lazy val benchmark = Project(
    id = "benchmark"
  , base = file("maestro-benchmark")
  , settings =
       standardSettings
    ++ uniform.project("maestro-benchmark", "au.com.cba.omnia.maestro.benchmark")
    ++ uniformThriftSettings
    ++ Seq[Sett](
      libraryDependencies ++= Seq(
        "com.github.axel22" %% "scalameter" % "0.4"
      ) ++ depend.testing()
    , testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework")
    , parallelExecution in Test := false
    , logBuffered := false
    )
  ).dependsOn(core)
   .dependsOn(macros)
   .dependsOn(api)

  def fixLicenses(old: String => MergeStrategy) =  (path: String) => path match {
    case f if f.toLowerCase.startsWith("meta-inf/license") => MergeStrategy.rename
    case "META-INF/NOTICE.txt" => MergeStrategy.rename
    case "META-INF/MANIFEST.MF" => MergeStrategy.discard
    case PathList("META-INF", xs) if xs.toLowerCase.endsWith(".dsa") => MergeStrategy.discard
    case PathList("META-INF", xs) if xs.toLowerCase.endsWith(".rsa") => MergeStrategy.discard
    case PathList("META-INF", xs) if xs.toLowerCase.endsWith(".sf") => MergeStrategy.discard
    case _ => MergeStrategy.first
  }
}
