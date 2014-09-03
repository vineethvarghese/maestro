package au.com.cba.omnia.maestro.core.task

import java.io.File
import com.cloudera.sqoop.SqoopOptions
import com.twitter.scalding.{ Args, Job, Mode }
import au.com.cba.omnia.parlour.{ ExportSqoopJob, ImportSqoopJob }
import cascading.flow.FlowDef
import au.com.cba.omnia.parlour.ParlourImportOptions
import au.com.cba.omnia.parlour.ParlourOptions
import au.com.cba.omnia.parlour.SqoopSyntax.ParlourImportDsl

trait Sqoop {
  def sqoopImport(
    options: ParlourImportOptions[_])(args: Args
  )(implicit flowDef: FlowDef, mode: Mode): (Job, List[String]) = {
    val sqoopOptions = options.toSqoopOptions
    (new ImportSqoopJob(sqoopOptions)(args), List(sqoopOptions.getTargetDir()))
  }

  def sqoopImport(
    hdfsRoot: String,
    source: String,
    domain: String,
    tableName: String,
    options: ParlourImportOptions[_]
  )(args: Args)(implicit flowDef: FlowDef, mode: Mode): (Job, List[String]) = {

    val hdfsLandingDir = List(hdfsRoot, "source", source, domain, tableName) mkString File.separator
    options.targetDir(hdfsLandingDir)
    (new ImportSqoopJob(options.toSqoopOptions)(args), List(hdfsLandingDir))
  }

  def sqoopImport[T <: ParlourImportOptions[T]](
    hdfsRoot: String,
    source: String,
    domain: String,
    tableName: String,
    connectionString: String,
    username: String,
    password: String,
    options: ParlourImportOptions[T] = new ParlourImportDsl()
  )(args: Args)(implicit flowDef: FlowDef, mode: Mode): (Job, List[String]) = {
    options.connectionString(connectionString)
      .username(username)
      .password(password)
      .tableName(tableName)
    sqoopImport(hdfsRoot, source, domain, tableName, options)(args)
  }

  def sqoopExport(options: SqoopOptions)(args: Args)(implicit flowDef: FlowDef, mode: Mode): Job = {
    new ExportSqoopJob(options)(args)
  }
}