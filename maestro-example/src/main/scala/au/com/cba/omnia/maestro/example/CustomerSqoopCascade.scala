
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

import com.twitter.scalding.Args

import au.com.cba.omnia.maestro.api.{MaestroCascade, Partition, RowFilter, UniqueJob, Validator}
import au.com.cba.omnia.maestro.api.Clean
import au.com.cba.omnia.maestro.api.Maestro.{load, now, view}
import au.com.cba.omnia.maestro.core.task.Sqoop
import au.com.cba.omnia.maestro.example.thrift.Customer
import au.com.cba.omnia.parlour.SplitByAmp
import au.com.cba.omnia.parlour.SqoopSyntax.TeradataParlourImportDsl

class CustomerSqoopCascade(args: Args) extends MaestroCascade[Customer](args) with Sqoop {
  val hdfsRoot = args("hdfs-root")
  val source = "teradata"
  val domain = "marketing"
  val tablename = "customer"
  val sqoopMappers = args("sqoop-mappers").toInt
  val dbHost = args("host")
  val dbDatabase = domain
  val dbUsername = args("username")
  val dbPassword = args("password")
  val errors = s"${hdfsRoot}/errors/${domain}"
  val filter = RowFilter.keep
  val cleaners = Clean.all(
    Clean.trim,
    Clean.removeNonPrintables)
  val validators = Validator.all[Customer]()
  val customerView = s"${hdfsRoot}/view/warehouse/${domain}/${tablename}"

  /**
   * In order for sqoop to work, you will need to include the teradata drivers and cloudera connector 
   * in the maestro-example/lib folder when building the assembly. 
   */
  /*val importOptions = TeradataParlourImportDsl()
    .connectionString(s"jdbc:teradata://${dbHost}/MODE=ANSI,CHARSET=UTF8,DATABASE=${dbDatabase}")
    .username(dbUsername)
    .password(dbPassword)
    .numberOfMappers(sqoopMappers)
    .inputMethod(SplitByAmp)
    .tableName(tablename)
    */
  val importOptions = TeradataParlourImportDsl()
  val (sqoopJob, imported) = sqoopImport(" ", " ", " ", hdfsRoot, source, domain, tablename, importOptions)(args)

  val jobs = Seq(
    sqoopJob,
    new UniqueJob(args) {
      val in  = load[Customer]("|", imported, errors, now(), cleaners, validators, filter, "null")
      val out = view(Partition.byField(Fields.Cat), customerView)(in)
    }
  )
}
