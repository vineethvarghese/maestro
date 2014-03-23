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
import org.apache.hadoop.hive.conf.HiveConf


case class Maestro(args: Args) {

  //TODO: have the view create the meta data.
  def view[A <: ThriftStruct : Manifest, B: Manifest: TupleSetter](partition: Partition[A, B], source: String, databaseName:String, output: String) =
    View.create(args, partition, source, databaseName, output)

  def load[A <: ThriftStruct : Decode : Tag : Manifest](delimiter: String, sources: List[String], output: String, errors: String, now: String, clean: Clean, validator: Validator[A], filter: RowFilter) =
    Load.create(args, delimiter, sources, output, errors, now, clean, validator, filter)

  def now(format: String = "yyyy-MM-dd") = {
    val f = new java.text.SimpleDateFormat(format)
    f.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))
    f.format(new java.util.Date)
  }


  //Create the Tap,create the flow, pass it to the job, return the new job
  def hql[I <: ThriftStruct : Manifest,O <: ThriftStruct : Manifest](name: String, database: String, query: String, properties: Map[HiveConf.ConfVars, String] = Map.empty[HiveConf.ConfVars,String]) = {
    //TODO: how do I derive the partition information at this level?
    val inputTaps = List(Hive.createHiveTap(Hive.createConf(properties), Hive.createDescriptor[I](database, ???), Hive.createScheme[I]))
    val outputTap = Hive.createHiveTap(Hive.createConf(properties), Hive.createDescriptor[O](database, ???), Hive.createScheme[O])
    Hive.create(args, name, query, inputTaps, outputTap)
  }


  def hql[I1 <: ThriftStruct : Manifest,I2 <: ThriftStruct : Manifest,O <: ThriftStruct : Manifest](name: String, database: String, query: String, properties: Map[HiveConf.ConfVars, String] = Map.empty[HiveConf.ConfVars,String]) = {
    val inputTaps = List(Hive.createHiveTap(Hive.createConf(properties), Hive.createDescriptor[I1](database, ???), Hive.createScheme[I1]),
                         Hive.createHiveTap(Hive.createConf(properties), Hive.createDescriptor[I2](database, ???), Hive.createScheme[I2]))
    val outputTap = Hive.createHiveTap(Hive.createConf(properties), Hive.createDescriptor[O](database, ???), Hive.createScheme[O])
    Hive.create(args, name, query, inputTaps, outputTap)

  }

  def hql[I1 <: ThriftStruct : Manifest,I2 <: ThriftStruct : Manifest, I3 <: ThriftStruct : Manifest,O <: ThriftStruct : Manifest](name: String, database: String, query: String, properties: Map[HiveConf.ConfVars, String] = Map.empty[HiveConf.ConfVars,String]) = {
    val inputTaps = List(Hive.createHiveTap(Hive.createConf(properties), Hive.createDescriptor[I1](database, ???), Hive.createScheme[I1]),
                         Hive.createHiveTap(Hive.createConf(properties), Hive.createDescriptor[I2](database, ???), Hive.createScheme[I2]),
                         Hive.createHiveTap(Hive.createConf(properties), Hive.createDescriptor[I3](database, ???), Hive.createScheme[I3]))
    val outputTap = Hive.createHiveTap(Hive.createConf(properties), Hive.createDescriptor[O](database, ???), Hive.createScheme[O])
    Hive.create(args, name, query, inputTaps, outputTap)
  }
}
