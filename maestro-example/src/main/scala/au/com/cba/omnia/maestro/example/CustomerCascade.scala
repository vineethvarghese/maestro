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

import scalaz.{Tag => _, _}, Scalaz._

import com.twitter.scalding._, TDsl._

import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.hive.conf.HiveConf.ConfVars._

import com.cba.omnia.edge.hdfs.{Error, Ok}

import au.com.cba.omnia.maestro.api._, Maestro._
import au.com.cba.omnia.maestro.core.codec._
import au.com.cba.omnia.maestro.example.thrift.Customer

class CustomerCascade(args: Args) extends MaestroCascade[Customer](args) {
  val hdfsRoot    = args("hdfs-root")
  val source      = "customer"
  val domain      = "customer"
  val tablename   = "customer"
  val localRoot   = args("local-root")
  val archiveRoot = args("archive-root")
  val errors      = s"${hdfsRoot}/errors/${domain}"
  val validators  = Validator.all[Customer]()
  val filter      = RowFilter.keep
  val cleaners    = Clean.all(
    Clean.trim,
    Clean.removeNonPrintables
  )
  val dateTable =
    HiveTable(domain, "by_date", Partition.byDate(Fields.EffectiveDate) )
  val catTable  =
    HiveTable(domain, "by_cat", Partition.byField(Fields.Cat))

  upload(source, domain, tablename, "{table}_{yyyyMMdd}.txt", localRoot, archiveRoot, hdfsRoot, new Configuration) match {
    case Ok(_)    => {}
    case Error(e) => throw new Exception(s"Failed to upload file to HDFS: $e")
  }

  val inputs = Guard.expandPaths(s"${hdfsRoot}/source/${source}/${domain}/${tablename}/*/*/*")

  val jobs = Seq(
    new UniqueJob(args) {
      load[Customer](
        "|", inputs, errors,
        Maestro.timeFromPath(".*/([0-9]{4})/([0-9]{2})/([0-9]{2}).*".r),
        cleaners, validators, filter
      ) |>
      (viewHive(dateTable) _ &&&
        viewHive(catTable)
      )
    },
    hiveQuery(
      args, "test",
      dateTable, Some(catTable),
      Map(HIVEMERGEMAPFILES -> "true"),
      s"INSERT OVERWRITE TABLE ${catTable.name} PARTITION (partition_cat) SELECT id, name, acct, cat, sub_cat, -10, effective_date, cat AS partition_cat FROM ${dateTable.name}",
      s"SELECT COUNT(*) FROM ${catTable.name}"
    )
  )
}

class SplitCustomerCascade(args: Args) extends Maestro[Customer](args) {
  val env           = args("env")
  val domain        = "customer"
  val inputs        = Guard.expandPaths(s"${env}/source/${domain}/*")
  val errors        = s"${env}/errors/${domain}"
  val dateView      = s"${env}/view/warehouse/${domain}/by-date"
  val catView       = s"${env}/view/warehouse/${domain}/by-cat"

  val cleaners      = Clean.all(
    Clean.trim,
    Clean.removeNonPrintables
  )

  val validators    = Validator.all(
    Validator.of(Fields.SubCat, Check.oneOf("M", "F")),
    Validator.by[Customer](_.acct.length == 4, "Customer accounts should always be a length of 4")
  )

  val filter        = RowFilter.keep
  val timeSource    = Maestro.timeFromPath(""".*(\d{4})-(\d{2})-(\d{2}).*""".r)

  val parti = Partition.byDate(Fields.EffectiveDate)
  load[Customer]("|", inputs, errors, timeSource, cleaners, validators, filter)
    .map(split[Customer,(Customer, Customer)]) >>*
  (
    view(Partition.byFields2(Fields.Cat, Fields.SubCat), catView),
    view(Partition.byDate(Fields.EffectiveDate), dateView)
  )
}
