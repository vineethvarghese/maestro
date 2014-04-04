package au.com.cba.omnia.maestro.api

import scalaz._, Scalaz._
import au.com.cba.omnia.maestro.core.codec._
import au.com.cba.omnia.maestro.core.clean._
import au.com.cba.omnia.maestro.core.validate._
import au.com.cba.omnia.maestro.core.task._
import au.com.cba.omnia.maestro.core.filter._
import au.com.cba.omnia.maestro.core.hive._
import com.twitter.scalding._, TDsl._
import com.twitter.scrooge._
import org.apache.hadoop.hive.conf.HiveConf

case class Maestro(env: String, args: Args) {
  
  def hiveConf = new HiveConf <| (_.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, s"/${env}/hive"))
  
  def view[A <: ThriftStruct : Manifest, B: Manifest: TupleSetter](descriptor: TableDescriptor[A, B], source: String) =
    View.create(env, args, hiveConf, descriptor, source)

  def load[A <: ThriftStruct : Decode : Tag : Manifest](delimiter: String, sources: List[String], output: String, errors: String, now: String, clean: Clean, validator: Validator[A], filter: RowFilter) =
    Load.create(args, delimiter, sources, output, errors, now, clean, validator, filter)

  def now(format: String = "yyyy-MM-dd") = {
    val f = new java.text.SimpleDateFormat(format)
    f.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))
    f.format(new java.util.Date)
  }

  // TODO: seperate out a query and view or just have a single call?
  def hqlQuery(name: String, inputDescriptors: List[TableDescriptor[_,_]], outputDescriptor: TableDescriptor[_,_], query: String) = {
    Hive.create(args, env, name, hiveConf, query, inputDescriptors, outputDescriptor)
  }
}
