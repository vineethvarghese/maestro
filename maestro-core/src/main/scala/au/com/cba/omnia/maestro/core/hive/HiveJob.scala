package au.com.cba.omnia.maestro.core.hive

import com.twitter.scalding._
import au.com.cba.omnia.maestro.core.scalding._
import org.apache.hadoop.hive.conf.HiveConf
import au.com.cba.omnia.maestro.core.partition.Partition
import com.twitter.scrooge.ThriftStruct
import cascading.scheme.Scheme
import au.com.cba.omnia.ebenezer.scrooge.ParquetScroogeScheme
import cascading.flow.hive.HiveFlow
import cascading.tap.hive.{HiveTap, HiveTableDescriptor, ParquetTableDescriptor}
import cascading.tap.SinkMode

object Hive {
  def create(args: Args, name : String, query: String, inputTaps: List[HiveTap], outputTap: HiveTap) = new HiveJob(args,name,query, inputTaps, outputTap)

  def createConf(properties: Map[HiveConf.ConfVars, String]) = {
    val conf = new HiveConf()
    for((key,value) <- properties)conf.setVar(key,value)
    conf
  }

  def createScheme[A <: ThriftStruct]() = new ParquetScroogeScheme[A]

  def createDescriptor[A <: ThriftStruct : Manifest](db: String, partition: Partition[A, B]):HiveTableDescriptor = {
    //TODO: extract the table name from the struct
    //TODO: extract the field names and types from the struct, map them onto hive types
    //TODO: extract the partition field names
    new ParquetTableDescriptor(db, "name", Array(""), Array(""), Array(""))
  }

  def createHiveTap(conf : HiveConf, descriptor : HiveTableDescriptor, scheme: Scheme) = {
    new HiveTap(conf, descriptor, scheme, SinkMode.REPLACE, true)
  }
}

class HiveJob(args: Args, name : String, query: String, inputTaps: List[HiveTap], outputTap: HiveTap) extends UniqueJob(args){
  
  override def buildFlow = {
    new HiveFlow(name, query, inputTaps, outputTap)
  }

}
