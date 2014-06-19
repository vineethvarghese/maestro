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

package au.com.cba.omnia.maestro.api

import scalaz.{Tag => _, _}, Scalaz._

import scala.util.matching.Regex

import cascading.flow.FlowDef

import com.twitter.scalding.{Hdfs =>_,_},TDsl._

import com.twitter.scrooge.ThriftStruct

import org.apache.hadoop.conf.Configuration

import au.com.cba.omnia.maestro.macros._
import au.com.cba.omnia.maestro.core.codec.{Decode, Tag}
import au.com.cba.omnia.maestro.core.task._
import au.com.cba.omnia.maestro.core.scalding.UnravelPipeImplicits

import au.com.cba.omnia.maestro.macros.SplitMacro

import com.cba.omnia.edge.hdfs._
import com.cba.omnia.edge.hdfs.HdfsString._

class Maestro[A <: ThriftStruct](args: Args) extends Job(args) with MacroSupport[A] {

}

object Maestro extends UnravelPipeImplicits with Load with View with Query {
  /**
    * Splits the given struct A into a tuple of smaller thrift structs by matching the field names.
    * 
    * It can duplicate the same original field across several structs, rearrange field order and skip fields.
    */
  def split[A <: ThriftStruct, B <: Product]: A => B =
    macro SplitMacro.impl[A, B]

  /** Use the current time yyyy-MM-dd as the load time for the data */
  def now(format: String = "yyyy-MM-dd") = {
    val f = new java.text.SimpleDateFormat(format)
    f.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))
    Predetermined(f.format(new java.util.Date))
  }

  /**
    * Derive the load time from the file path using the provided regex.
    * The regex needs to extract the year,month and day into separate capture groups.
    */
  def timeFromPath(regex: Regex): TimeSource = FromPath { (path: String) =>
    val m = regex.pattern.matcher(path)
    m.matches
    s"${m.group(1)}-${m.group(2)}-${m.group(3)}"
  }

  def timeFromPathHour(regex: Regex): TimeSource = FromPath { (path: String) =>
    val m = regex.pattern.matcher(path)
    m.matches
    s"${m.group(1)}-${m.group(2)}-${m.group(3)}-${m.group(4)}"
  }
  /**
    * Creates the _PROCESSED flag to indicate completion of processing in given list of paths
    */
  def createFlagFile(directoryPath : List[String]):Unit={
    directoryPath foreach ((x:String)=> Hdfs.create(Hdfs.path(s"$x/_PROCESSED")).run(new Configuration))
  }  
}
