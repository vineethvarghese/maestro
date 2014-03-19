package au.com.cba.omnia.maestro.api

import au.com.cba.omnia.maestro.core.data._
import au.com.cba.omnia.maestro.core.partition._
import au.com.cba.omnia.maestro.core.codec._
import au.com.cba.omnia.maestro.core.clean._
import au.com.cba.omnia.maestro.core.validate._
import au.com.cba.omnia.maestro.core.task._
import au.com.cba.omnia.maestro.core.filter._
import au.com.cba.omnia.maestro.core.hive._
import com.twitter.scalding._, TDsl._
import com.twitter.scrooge._


case class Maestro(args: Args) {
  def view[A <: ThriftStruct : Manifest, B: Manifest: TupleSetter](partition: Partition[A, B], source: String, output: String) =
    View.create(args, partition, source, output)

  def load[A <: ThriftStruct : Decode : Tag : Manifest](delimiter: String, sources: List[String], output: String, errors: String, now: String, clean: Clean, validator: Validator[A], filter: RowFilter) =
    Load.create(args, delimiter, sources, output, errors, now, clean, validator, filter)

  def now(format: String = "yyyy-MM-dd") = {
    val f = new java.text.SimpleDateFormat(format)
    f.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))
    f.format(new java.util.Date)
  }

  //TODO: update the properties to be Map[org.apache.hadoop.hive.conf.HiveConf.ConfVars, String]
  //TODO: take these configs all the way through to the job, use the overloaded constructor
  def hql(name :  String, query: String, properties: Map[String, String] = Map.empty[String,String]) = {
    Hive.create(args,name,query, properties)
  }

  def hql(query: String) = {
    Hive.create(args,query.substring(0,20) + "...", query, Map.empty[String,String])
  }

  def hql(query: String, properties: Map[String, String]) = {                                                                                                                                                                                          
    Hive.create(args,query.substring(0,20) + "...", query, properties)                                                                                                                                                                                                            
  } 
}
