package au.com.cba.omnia.maestro.api

import scala.util.matching.Regex

import com.twitter.scalding._, TDsl._
import com.twitter.scalding.{Args, TupleSetter}

import com.twitter.scrooge._
import com.twitter.scrooge.ThriftStruct

import au.com.cba.omnia.maestro.core.data._
import au.com.cba.omnia.maestro.core.partition._

import scalaz._, Scalaz._

import au.com.cba.omnia.maestro.core.codec._
import au.com.cba.omnia.maestro.core.clean._
import au.com.cba.omnia.maestro.core.validate._
import au.com.cba.omnia.maestro.core.task._
import au.com.cba.omnia.maestro.core.filter._
import au.com.cba.omnia.maestro.core.hive._

import org.apache.hadoop.hive.conf.HiveConf

case class Maestro(env: String, args: Args) {
  
  def hiveConf = new HiveConf <| (_.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, s"/${env}/hive"))
  
  def view[A <: ThriftStruct : Manifest, B: Manifest: TupleSetter](descriptor: TableDescriptor[A, B], source: String) =
    View.create(env, args, hiveConf, descriptor, source)

  def load[A <: ThriftStruct : Decode : Tag : Manifest](delimiter: String, sources: List[String], output: String, errors: String, now: TimeSource, clean: Clean, validator: Validator[A], filter: RowFilter) =
    Load.create(args, delimiter, sources, output, errors, now, clean, validator, filter)

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

  // TODO: seperate out a query and view or just have a single call?
  def hqlQuery(name: String, inputDescriptors: List[TableDescriptor[_,_]], outputDescriptor: TableDescriptor[_,_], query: String) = {
    Hive.create(args, env, name, hiveConf, query, inputDescriptors, outputDescriptor)
  }
}
