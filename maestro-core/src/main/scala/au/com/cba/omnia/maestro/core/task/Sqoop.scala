package au.com.cba.omnia.maestro.core.task

import java.io.File
import com.twitter.scalding._, TDsl._, Dsl._
import org.apache.log4j.Logger
import org.joda.time.{DateTime, DateTimeZone}
import au.com.cba.omnia.parlour.{ExportSqoopJob, ImportSqoopJob, ParlourExportOptions, ParlourImportOptions}
import au.com.cba.omnia.parlour.SqoopSyntax.{ParlourExportDsl, ParlourImportDsl}
import cascading.flow.FlowDef
import com.twitter.scalding.MultipleTextLineFiles
import cascading.tap.Tap


/**
  * Import and export data between a database and HDFS.
  * 
  * Methods return a list of jobs which can be added to a cascade.
  * 
  * See the example at `au.com.cba.omnia.maestro.example.CustomerSqoopExample`.
  * 
  * For a given `domain` and `tableName`, [[sqoopImport]] imports data files 
  * to the standard HDFS location: `\$hdfsRoot/source/\$source/\$domain/\$tableName`.
  *
  * Only use [[customSqoopImport]] if we are required to use non-standard target location.
  * 
  * [[sqoopExport]] assumes export dir data is in PSV format.
  */
trait Sqoop {
  /**
   * Run a sqoop import using custom import options
   * 
   * @param options: Custom import options
   */
  def customSqoopImport(
    options: ParlourImportOptions[_])(args: Args
  )(implicit flowDef: FlowDef, mode: Mode): (List[Job], List[String]) = {
    val logger = Logger.getLogger("Sqoop")
    val sqoopOptions = options.toSqoopOptions
    logger.info("Start of sqoop import")
    logger.info(s"connectionString = ${sqoopOptions.getConnectString}")
    logger.info(s"tableName        = ${sqoopOptions.getTableName}")
    logger.info(s"targetDir        = ${sqoopOptions.getTargetDir}")

    val job = new ImportSqoopJob(sqoopOptions)(args)
    (List(job), List(sqoopOptions.getTargetDir))
  }
  
  def customSqoopImport2(
    nextJob: Job, 
    options: ParlourImportOptions[_]
  )(args: Args)(implicit flowDef: FlowDef, mode: Mode): Job = {
    val logger = Logger.getLogger("Sqoop")
    val sqoopOptions = options.toSqoopOptions
    logger.info("Start of sqoop import")
    logger.info(s"connectionString = ${sqoopOptions.getConnectString}")
    logger.info(s"tableName        = ${sqoopOptions.getTableName}")
    logger.info(s"targetDir        = ${sqoopOptions.getTargetDir}")
    val sink:Tap[_,_,_] = nextJob.buildFlow.getSourcesCollection().iterator().next()
    new ImportSqoopJob(sqoopOptions, sink)(args)
  }

  /**
    * Runs a sqoop import from a database to HDFS.
    *
    * Data will be copied onto HDFS at the following directory:
    * `\$hdfsRoot/source/\$source/\$domain/\$tableName/<year>/<month>/<day>/`.
    *
    * Year, month and day are derived from the current date.
    * 
    * @param hdfsRoot: Root directory of HDFS
    * @param source: Source system
    * @param domain: Database or project within source
    * @param tableName: Table name or file name in database or project
    * @param options: Extra import options
    * @return Tuple of list of jobs for this import and list of imported directories.
    */
  def sqoopImport[T <: ParlourImportOptions[T]](
    hdfsRoot: String,
    source: String,
    domain: String,
    tableName: String,
    options: ParlourImportOptions[T] = ParlourImportDsl()
  )(args: Args)(implicit flowDef: FlowDef, mode: Mode): (List[Job], List[String]) = {
    val date = DateTime.now(DateTimeZone.UTC).toString(List("yyyy","MM", "dd") mkString File.separator)
    val hdfsLandingDir = List(hdfsRoot, "source", source, domain, tableName, date) mkString File.separator
    options.targetDir(hdfsLandingDir)
    customSqoopImport(options)(args)
  }

  /**
    * Runs a sqoop import from a database to HDFS.
    *
    * Data will be copied into HDFS at the following directory:
    * `\$hdfsRoot/source/\$source/\$domain/\$tableName/<year>/<month>/<day>/`.
    *
    * Year, month and day are derived from the current date.
    * 
    * @param hdfsRoot: Root directory of HDFS
    * @param source: Source system
    * @param domain: Database or project within source
    * @param tableName: Table name or file name in database or project
    * @param connectionString: Jdbc url for connecting to the database
    * @param username: Username for connecting to the database
    * @param password: Password for connecting to the database
    * @param options: Extra import options 
    * @return Tuple of list of jobs for this import and list of imported directories
    */
  def sqoopImport[T <: ParlourImportOptions[T]](
    hdfsRoot: String,
    source: String,
    domain: String,
    tableName: String,
    connectionString: String,
    username: String,
    password: String,
    options: ParlourImportOptions[T] = ParlourImportDsl()
  )(args: Args)(implicit flowDef: FlowDef, mode: Mode): (List[Job], List[String]) = {
    options.connectionString(connectionString)
      .username(username)
      .password(password)
      .tableName(tableName)
    sqoopImport(hdfsRoot, source, domain, tableName, options)(args)
  }

  /**
    * Runs a sqoop export from HDFS to a database.
    *
    * @param options: Custom export options 
    * @return List of jobs for this export
    */
  def customSqoopExport[T <: ParlourExportOptions[T]](
    options: ParlourExportOptions[T]
  )(args: Args)(implicit flowDef: FlowDef, mode: Mode): List[Job] = {
    List(new ExportSqoopJob(options.toSqoopOptions)(args))
  }

  /**
    * Runs a sqoop export from HDFS to a database.
    *
    * @param exportDir: Directory containing data to be exported
    * @param tableName: Table name or file name in database or project
    * @param connectionString: Jdbc url for connecting to the database
    * @param username: Username for connecting to the database
    * @param password: Password for connecting to the database
    * @param options: Extra export options 
    * @return List of jobs for this export
    */
  def sqoopExport[T <: ParlourExportOptions[T]](
    exportDir: String,
    tableName: String,
    connectionString: String,
    username: String,
    password: String,
    options: ParlourExportOptions[T] = ParlourExportDsl()
  )(args: Args)(implicit flowDef: FlowDef, mode: Mode): List[Job] = {
    options.exportDir(exportDir)
      .tableName(tableName)
      .connectionString(connectionString)
      .username(username)
      .password(password)
    customSqoopExport(options)(args)
  }
}
