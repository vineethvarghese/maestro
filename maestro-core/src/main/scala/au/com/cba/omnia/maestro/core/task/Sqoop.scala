//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package au.com.cba.omnia.maestro.core.task

import java.io.File

import org.apache.log4j.Logger

import cascading.flow.FlowDef

import org.apache.hadoop.io.compress.BZip2Codec

import com.twitter.scalding.{Args, Job, Mode, TextLine, TypedPipe}

import com.cba.omnia.edge.source.compressible.CompressibleTypedTsv

import au.com.cba.omnia.parlour.SqoopSyntax.{ParlourExportDsl, ParlourImportDsl}
import au.com.cba.omnia.parlour._

import au.com.cba.omnia.maestro.core.args.ModArgs

/**
 * Import and export data between a database and HDFS.
 *
 * All methods return Jobs that can be added to a cascade.
 *
 * See the example at `au.com.cba.omnia.maestro.example.CustomerSqoopExample` to understand how to use
 * the [[Sqoop]] API
 */
trait Sqoop {

  /**
   * Archive Job for data imported by sqoop
   *
   * @param importPath: Path pointing to the folder to archive
   * @param archivePath: Path pointing to the archive location
   */
  protected class ArchiveDirectoryJob(importPath: String, archivePath: String)(args: Args)
      extends Job(ModArgs.compressOutput[BZip2Codec].modify(args)) {

    TypedPipe.from(TextLine(importPath)).write(CompressibleTypedTsv[String](archivePath))

    override def validate = () // default scalding validate chokes with cascades
  }

  /**
   * Convenience method to populate a parlour import option instance
   *
   * @param tableName: table name
   * @param connectionString: database connection string
   * @param username: database username
   * @param password: database password
   * @param outputFieldsTerminatedBy: output field terminating character
   * @param whereCondition: where condition if any
   * @param options: parlour option to populate
   * @return : Populated parlour option
   */
  def createSqoopImportOptions[T <: ParlourImportOptions[T]](
    tableName: String,
    connectionString: String,
    username: String,
    password: String,
    outputFieldsTerminatedBy: Char,
    whereCondition: Option[String] = None,
    options: T = ParlourImportDsl()
  ): T = {
    val withConnection = options.connectionString(connectionString).username(username).password(password)
    val withEntity = withConnection.tableName(tableName).fieldsTerminatedBy(outputFieldsTerminatedBy)
    whereCondition.map(withEntity.where).getOrElse(withEntity)
  }

  /**
   * Run a custom sqoop import using parlour import options
   *
   * '''Use this method ONLY if non-standard settings are required'''
   *
   * @param options: Parlour import options
   */
  def customSqoopImport(
    options: ParlourImportOptions[_]
  )(args: Args)(implicit flowDef: FlowDef, mode: Mode): Job = {
    val logger = Logger.getLogger("Sqoop")
    val sqoopOptions = options.toSqoopOptions
    logger.info("Start of sqoop import")
    logger.info(s"connectionString = ${sqoopOptions.getConnectString}")
    logger.info(s"tableName        = ${sqoopOptions.getTableName}")
    logger.info(s"targetDir        = ${sqoopOptions.getTargetDir}")
    new ImportSqoopJob(sqoopOptions)(args)
  }

  /**
   * Runs a sqoop import from a database table to HDFS.
   *
   * Data will be copied to a path that is generated. For a given `domain`,`tableName` and `timePath`, a path
   * `\$hdfsRoot/source/\$source/\$domain/\$tableName/\$timePath` is generated.
   *
   * @param hdfsRoot: Root directory of HDFS
   * @param source: Source system
   * @param domain: Database within source
   * @param tableName: Table name in database
   * @param timePath: custom timepath to import data into
   * @param options: Sqoop import options
   * @return Tuple of Seq of jobs for this import and import directory.
   */
  def sqoopImport[T <: ParlourImportOptions[T]](
    hdfsRoot: String,
    source: String,
    domain: String,
    tableName: String,
    timePath: String,
    options: ParlourImportOptions[T] = ParlourImportDsl()
  )(args: Args)(implicit flowDef: FlowDef, mode: Mode): (Seq[Job], String) = {
    val importPath = List(hdfsRoot, "source", source, domain, tableName, timePath) mkString File.separator
    val archivePath = List(hdfsRoot, "archive", source, domain, tableName, timePath) mkString File.separator
    val finalOptions = options.tableName(tableName).targetDir(importPath)
    (Seq(customSqoopImport(finalOptions)(args),
      new ArchiveDirectoryJob(importPath, archivePath)(args)), importPath)
  }

  /**
   * Runs a sqoop export from HDFS to a database table.
   *
   * @param options: Custom export options
   * @return Job for this export
   */
  def customSqoopExport[T <: ParlourExportOptions[T]](
    options: ParlourExportOptions[T]
  )(args: Args)(implicit flowDef: FlowDef, mode: Mode): Job = {
    new ExportSqoopJob(options.toSqoopOptions)(args)
  }

  /**
   * Runs a sqoop export from HDFS to a database table.
   *
   * @param exportDir: Directory containing data to be exported
   * @param tableName: Table name in the database
   * @param connectionString: Jdbc url for connecting to the database
   * @param username: Username for connecting to the database
   * @param password: Password for connecting to the database
   * @param options: Extra export options
   * @return Job for this export
   */
  def sqoopExport[T <: ParlourExportOptions[T]](
    exportDir: String,
    tableName: String,
    connectionString: String,
    username: String,
    password: String,
    inputFieldsTerminatedBy: Char,
    options: ParlourExportOptions[T] = ParlourExportDsl()
  )(args: Args)(implicit flowDef: FlowDef, mode: Mode): Job = {
    val withConnection = options.connectionString(connectionString).username(username).password(password)
    val withEntity = withConnection.exportDir(exportDir).tableName(tableName).inputFieldsTerminatedBy(inputFieldsTerminatedBy)
    customSqoopExport(withEntity)(args)
  }
}
