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

package au.com.cba.omnia.maestro.core
package task

import java.io.File

import scala.util.matching.Regex

import org.apache.hadoop.conf.Configuration

import org.apache.log4j.Logger

import scalaz._, Scalaz._, scalaz.\&/.{This, That, Both}

import com.cba.omnia.edge.hdfs.{Error, Hdfs, Ok, Result}

import au.com.cba.omnia.maestro.core.upload._

/**
  * Push source files to HDFS using [[upload]] and archive them.
  *
  * See the example at `au.com.cba.omnia.maestro.example.CustomerUploadExample`.
  *
  * In order to run map-reduce jobs, we first need to get our data onto HDFS.
  * [[upload]] copies data files from the local machine onto HDFS and archives
  * the files.
  *
  * For a given `domain` and `tableName`, [[upload]] copies data files from
  * the standard local location: `\$sourceRoot/dataFeed/\$domain`, to the
  * standard HDFS location: `\$hdfsRoot/source/\$domain/\$tableName`.
  *
  * Only use [[customUpload]] if we are required to use non-standard locations.
  */
trait Upload {

  /**
    * Pushes source files onto HDFS and archives them locally.
    *
    * `upload` expects data files intended for HDFS to be placed in
    * the local folder `\$sourceRoot/dataFeed/\$source/\$domain`. Different
    * source systems will use different values for `source` and `domain`.
    * `upload` processes all data files that match a given file pattern.
    * The file pattern format is explained below.
    *
    * Each data file will be copied onto HDFS as the following file:
    * `\$hdfsRoot/source/\$source/\$domain/\$tableName/<year>/<month>/<day>/<originalFileName>`.
    *
    * Data files are also gzipped and archived on the local machine and on HDFS.
    * Each data file is archived as:
    *  - `\$archiveRoot/\$source/\$domain/\$tableName/<year>/<month>/<day>/<originalFileName>.gz`
    *  - `\$hdfsRoot/archive/\$source/\$domain/\$tableName/<year>/<month>/<day>/<originalFileName>.gz`
    *
    * (These destination paths may change slightly depending on the fields in the file pattern.)
    *
    * Some files placed on the local machine are control files. These files
    * are not intended for HDFS and are ignored by `upload`. `upload` will log
    * a message whenever it ignores a control file.
    *
    * When an error occurs, `upload` stops copying files immediately. Once the
    * cause of the error has been addressed, `upload` can be run again to copy
    * any remaining files to HDFS. `upload` will refuse to copy a file if that
    * file or it's control flags are already present in HDFS. If you
    * need to overwrite a file that already exists in HDFS, you will need to
    * delete the file and it's control flags before `upload` will replace it.
    *
    * The file pattern is a string containing the following elements:
    *  - Literals:
    *    - Any character other than `{`, `}`, `*`, `?`, or `\` represents itself.
    *    - `\{`, `\}`, `\*`, `\?`, or `\\` represents `{`, `}`, `*`, `?`, and `\`, respectively.
    *    - The string `{table}` represents the table name.
    *  - Wildcards:
    *    - The character `*` represents an arbitrary number of arbitrary characters.
    *    - The character `?` represents one arbitrary character.
    *  - Date times:
    *    - The string `{<timestamp-pattern>}` represents a JodaTime timestamp pattern.
    *    - `upload` only supports certain date time fields:
    *      - year (y),
    *      - month of year (M),
    *      - day of month (d),
    *      - hour of day (H),
    *      - minute of hour (m), and
    *      - second of minute (s).
    *
    * Some example file patterns:
    *  - `{table}{yyyyMMdd}.DAT`
    *  - `{table}_{yyyyMMdd_HHss}.TXT.*.{yyyyMMddHHss}`
    *  - `??_{table}-{ddMMyy}*`
    *
    * @param source: Source system
    * @param domain: Database or project within source
    * @param tableName: Table name or file name in database or project
    * @param filePattern: File name pattern
    * @param sourceRoot: Root directory of incoming data files
    * @param archiveRoot: Root directory of the local archive
    * @param hdfsRoot: Root directory of HDFS
    * @param conf: Hadoop configuration
    * @return Any error occuring when uploading files
    */
  def upload(
    source: String, domain: String, tableName: String, filePattern: String,
    sourceRoot: String, archiveRoot: String, hdfsRoot: String,
    conf: Configuration, controlPattern: Regex = ControlPattern.default
  ): Result[Unit] = {
    val logger = Logger.getLogger("Upload")

    logger.info("Start of upload")
    logger.info(s"source      = $source")
    logger.info(s"domain      = $domain")
    logger.info(s"tableName   = $tableName")
    logger.info(s"filePattern = $filePattern")
    logger.info(s"sourceRoot  = $sourceRoot")
    logger.info(s"archiveRoot = $archiveRoot")
    logger.info(s"hdfsRoot    = $hdfsRoot")

    val locSourceDir   = List(sourceRoot,  "dataFeed", source, domain)            mkString File.separator
    val archiveDir     = List(archiveRoot,             source, domain, tableName) mkString File.separator
    val hdfsArchiveDir = List(hdfsRoot,    "archive",  source, domain, tableName) mkString File.separator
    val hdfsLandingDir = List(hdfsRoot,    "source",   source, domain, tableName) mkString File.separator

    val result: Result[Unit] =
      Upload.uploadImpl(tableName, filePattern, locSourceDir, archiveDir, hdfsArchiveDir, hdfsLandingDir, controlPattern).safe.run(conf)

    val args = s"$source/$domain/$tableName"
    result match {
      case Ok(())                => logger.info(s"Upload ended for $args")
      case Error(This(msg))      => logger.error(s"Upload failed for $args: $msg")
      case Error(That(exn))      => logger.error(s"Upload failed for $args", exn)
      case Error(Both(msg, exn)) => logger.error(s"Upload failed for $args: $msg", exn)
    }

    result
  }

  /**
    * Pushes source files onto HDFS and archives them locally, using non-standard file locations.
    *
    * As per [[upload]], except the user has more control where to find data
    * files, where to copy them, and where to archive them.
    *
    * Data files are found in the local folder `\$locSourceDir`. They are copied
    * to `\$hdfsLandingDir/<year>/<month>/<originalFileName>`,
    * and archived at `\$archiveDir/<year>/<month>/<originalFileName>.gz`. (These
    * directories may change slightly depending on the timestamp format.)
    *
    * In all other respects `customUpload` behaves the same as [[upload]].
    *
    * @param tableName: Table name or file name in database or project
    * @param filePattern: File name pattern
    * @param locSourceDir: Local source landing directory
    * @param archiveDir: Local archive directory
    * @param hdfsArchiveDir: HDFS archive directory
    * @param hdfsLandingDir: HDFS landing directory
    * @param conf: Hadoop configuration
    * @return Any error occuring when uploading files
    */
  def customUpload(
    tableName: String, filePattern: String, locSourceDir: String,
    archiveDir: String, hdfsArchiveDir: String, hdfsLandingDir: String,
    conf: Configuration, controlPattern: Regex = ControlPattern.default
  ): Result[Unit] = {
    val logger = Logger.getLogger("Upload")

    logger.info("Start of custom upload")
    logger.info(s"tableName      = $tableName")
    logger.info(s"filePattern    = $filePattern")
    logger.info(s"locSourceDir   = $locSourceDir")
    logger.info(s"archiveDir     = $archiveDir")
    logger.info(s"hdfsArchiveDir = $hdfsArchiveDir")
    logger.info(s"hdfsLandingDir = $hdfsLandingDir")

    val result =
      Upload.uploadImpl(tableName, filePattern, locSourceDir, archiveDir, hdfsArchiveDir, hdfsLandingDir, controlPattern).safe.run(conf)

    result match {
      case Ok(())                => logger.info(s"Custom upload ended from $locSourceDir")
      case Error(This(msg))      => logger.error(s"Custom upload failed from $locSourceDir: $msg")
      case Error(That(exn))      => logger.error(s"Custom upload failed from $locSourceDir", exn)
      case Error(Both(msg, exn)) => logger.error(s"Custom upload failed from $locSourceDir: $msg", exn)
    }

    result
  }
}

/**
  * Contains implementation for `upload` methods in [[Upload]] trait.
  *
  * WARNING: The methods on this object are not considered part of the public
  * maestro API, and may change without warning. Use the methods in the maestro
  * API instead, unless you know what you are doing.
  */
object Upload {
  val logger = Logger.getLogger("Upload")

  /** Implementation of `upload` methods in [[Upload]] trait */
  def uploadImpl(
    tableName: String, filePattern: String, locSourceDir: String,
    archiveDir: String, hdfsArchiveDir: String, hdfsLandingDir: String,
    controlPattern: Regex
  ): Hdfs[Unit] =
    for {
      inputFiles <- Hdfs.result(Input.findFiles(new File(locSourceDir), tableName, filePattern, controlPattern))

      _ <- inputFiles traverse_ {
        case Control(file)   => Hdfs.value(logger.info(s"skipping control file ${file.getName}"))
        case src @ Data(_,_) => for {
          copied <- Push.push(src, hdfsLandingDir, archiveDir, hdfsArchiveDir)
          _      =  logger.info(s"copied ${copied.source.getName} to ${copied.dest}")
        } yield ()
      }
    } yield ()
}
