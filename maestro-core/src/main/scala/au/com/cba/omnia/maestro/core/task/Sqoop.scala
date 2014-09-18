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

import cascading.flow.FlowDef
import com.twitter.scalding._
import org.apache.log4j.Logger
import org.joda.time.{DateTime, DateTimeZone}

import au.com.cba.omnia.parlour.{ExportSqoopJob, ImportSqoopJob, ParlourExportOptions, ParlourImportOptions}
import au.com.cba.omnia.parlour.SqoopSyntax.{ParlourExportDsl, ParlourImportDsl}

/**
 * Import and export data between a database and HDFS.
 *
 * All methods return a Job that can be added to a cascade.
 *
 * See the example at `au.com.cba.omnia.maestro.example.CustomerSqoopExample` to understand how to use
 * the [[Sqoop]] API
 *
 */
trait Sqoop {

  /**
   * For a given `domain` and `tableName`, [[ImportPath]] creates a standard
   * HDFS location: `\$hdfsRoot/source/\$source/\$domain/\$tableName/<year>/<month>/<day>`.
   *
   * Year, month and day are derived from the current date.
   *
   * @param hdfsRoot
   * @param source
   * @param domain
   * @param tableName
   */
  case class ImportPath(hdfsRoot: String, source: String, domain: String, tableName: String) {
    def path = {
      val date = DateTime.now(DateTimeZone.UTC).toString(List("yyyy","MM", "dd") mkString File.separator)
      List(hdfsRoot, "source", source, domain, tableName, date) mkString File.separator
    }
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
   * Runs a sqoop import from a database to HDFS.
   *
   * Data will be copied to the provided [[ImportPath]]
   *
   * @param importPath: Path to import the data to
   * @param tableName: Table name in the database
   * @param connectionString: Jdbc url for connecting to the database
   * @param username: Username for connecting to the database
   * @param password: Password for connecting to the database
   * @param outputFieldsTerminatedBy: Marker to terminate output fields with
   * @param whereCondition: Filter condition to be used on the table
   * @param options: Extra import options
   * @return Job for this import
   */
  def sqoopImport[T <: ParlourImportOptions[T]](
    importPath: ImportPath,
    tableName: String,
    connectionString: String,
    username: String,
    password: String,
    outputFieldsTerminatedBy: Char,
    whereCondition: Option[String] = None,
    options: ParlourImportOptions[T] = ParlourImportDsl()
  )(args: Args)(implicit flowDef: FlowDef, mode: Mode): Job = {
    val withConnection = options.connectionString(connectionString).username(username).password(password)
    val withEntity = withConnection.tableName(tableName).targetDir(importPath.path).fieldsTerminatedBy(outputFieldsTerminatedBy)
    val finalOptions = whereCondition.map(withEntity.where).getOrElse(withEntity)
    customSqoopImport(finalOptions)(args)
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
   * @return List of jobs for this export
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
