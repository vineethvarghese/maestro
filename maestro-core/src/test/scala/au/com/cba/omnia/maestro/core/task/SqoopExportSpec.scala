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

import scala.io.Source

import com.twitter.scalding.{Args, CascadeJob}

import org.specs2.specification.BeforeExample

import scalikejdbc._

import au.com.cba.omnia.parlour.SqoopSyntax.ParlourExportDsl

import au.com.cba.omnia.thermometer.core.Thermometer._
import au.com.cba.omnia.thermometer.core.ThermometerSpec

import au.com.cba.omnia.maestro.core.task.CustomerExport._

class SqoopExportSpec extends ThermometerSpec with BeforeExample { def is = s2"""
  Sqoop Export Cascade test
  =========================

  Export data from HDFS to DB $endToEndExportTest

"""
  val connectionString = "jdbc:hsqldb:mem:sqoopdb"
  val username = "sa"
  val password = ""
  val userHome = System.getProperty("user.home")

  def endToEndExportTest = {
    val resourceUrl = getClass.getResource("/sqoop")
    val data = Source.fromFile(s"${resourceUrl.getPath}/sales/books/customers/customer.txt").getLines().toSeq

    withEnvironment(path(resourceUrl.toString)) {
      withArgs(
        Map(
          "exportDir"        -> s"$dir/user/sales/books/customers",
          "exportTableName"  -> "customer_export",
          "mapRedHome"       -> s"$userHome/.ivy2/cache",
          "connectionString" -> connectionString,
          "username"         -> username,
          "password"         -> password
        )
      )(new SqoopExportCascade(_)).run
      tableData(connectionString, username, password) must containTheSameElementsAs(data)
    }
  }

  override def before: Any = tableSetup(connectionString, username, password)
}

class SqoopExportCascade(args: Args) extends CascadeJob(args) with Sqoop {
  val exportDir        = args("exportDir")
  val exportTableName  = args("exportTableName")
  val mappers          = 1
  val connectionString = args("connectionString")
  val username         = args("username")
  val password         = args("password")
  val mapRedHome       = args("mapRedHome")

  /**
   * hadoopMapRedHome is set for Sqoop to find the hadoop jars. This hack would be necessary ONLY in a
   * test case.
   */
  val exportOptions = ParlourExportDsl().hadoopMapRedHome(mapRedHome)

  override val jobs = Seq(sqoopExport(exportDir, exportTableName, connectionString, username, password, '|', exportOptions)(args))
}

object CustomerExport {

  Class.forName("org.hsqldb.jdbcDriver")

  def tableSetup(connectionString: String, username: String, password: String): Unit = {
    ConnectionPool.singleton(connectionString, username, password)
    implicit val session = AutoSession

    sql"""
      create table customer_export (
        id integer,
        name varchar(20),
        accr varchar(20),
        cat varchar(20),
        sub_cat varchar(20),
        balance integer
      )
    """.execute.apply()
  }

  def tableData(connectionString: String, username: String, password: String): List[String] = {
    ConnectionPool.singleton(connectionString, username, password)
    implicit val session = AutoSession
    sql"select * from customer_export".map(rs => List(rs.int("id"), rs.string("name"), rs.string("accr"),
      rs.string("cat"), rs.string("sub_cat"), rs.int("balance")) mkString "|").list.apply()
  }
}


