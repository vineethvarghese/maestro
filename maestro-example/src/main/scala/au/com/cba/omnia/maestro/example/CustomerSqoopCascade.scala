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
package au.com.cba.omnia.maestro.example

import java.io.File

import com.twitter.scalding.Args
import scalaz.{Tag => _, _}, Scalaz._

import org.joda.time.{DateTime, DateTimeZone}

import au.com.cba.omnia.maestro.api._, Maestro._
import au.com.cba.omnia.maestro.example.thrift.Customer

import au.com.cba.omnia.parlour.SplitByAmp
import au.com.cba.omnia.parlour.SqoopSyntax.{TeradataParlourExportDsl, TeradataParlourImportDsl}

class CustomerSqoopCascade(args: Args) extends MaestroCascade[Customer](args) {
  val hdfsRoot         = args("hdfs-root")
  val source           = args("source")
  val domain           = args("domain")
  val importTableName  = args("importTableName")
  val exportTableName  = args("exportTableName")
  val mappers          = args("mappers").toInt
  val host             = args("host")
  val database         = domain
  val connectionString = s"jdbc:teradata://${host}/MODE=ANSI,CHARSET=UTF8,DATABASE=${database}"
  val username         = args("username")
  val password         = args("password")
  val errors           = s"${hdfsRoot}/errors/${domain}"
  val filter           = RowFilter.keep
  val cleaners         = Clean.all(Clean.trim, Clean.removeNonPrintables)
  val validators       = Validator.all[Customer]()
  val customerView     = s"${hdfsRoot}/view/warehouse/${domain}/${importTableName}"
  val timePath         = DateTime.now(DateTimeZone.UTC).toString(List("yyyy","MM", "dd") mkString File.separator)

  /**
   * In order for sqoop to work with Teradata, you will need to include the teradata drivers and cloudera connector 
   * in the maestro-example/lib folder when building the assembly. 
   */
  val initialImportOptions = createSqoopImportOptions(importTableName, connectionString, username, password, '|', Some("1=1"), TeradataParlourImportDsl())
  val finalImportOptions = initialImportOptions.numberOfMappers(mappers).inputMethod(SplitByAmp).splitBy("id").verbose()

  val (importJobs, importPath) = sqoopImport(hdfsRoot, source, domain, importTableName, timePath, finalImportOptions)(args)
  val loadView = new UniqueJob(args) {
    load[Customer]("|", List(importPath), errors, now(), cleaners, validators, filter, "null") |>
    view(Partition.byField(Fields.Cat), customerView)
  }

  val exportOptions = TeradataParlourExportDsl().verbose()

  val exportJob = sqoopExport(importPath, exportTableName, connectionString, username, password, '|', exportOptions)(args)

  override val jobs = importJobs :+ loadView :+ exportJob
}


