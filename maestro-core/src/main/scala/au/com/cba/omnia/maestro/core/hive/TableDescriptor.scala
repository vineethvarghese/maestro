package au.com.cba.omnia.maestro.core
package hive

import scalaz._, Scalaz._
import au.com.cba.omnia.maestro.core.partition.Partition
import org.apache.thrift.protocol.{TType, TField}
import com.twitter.scrooge.ThriftStruct
import au.com.cba.omnia.maestro.core.codec.Describe
import cascading.tap.hive.HiveTableDescriptor
import com.twitter.scalding.TupleSetter
import au.com.cba.omnia.beehaus.ParquetTableDescriptor
import au.com.cba.omnia.ebenezer.scrooge.ParquetScroogeScheme
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf
import au.com.cba.omnia.ebenezer.scrooge.hive.Util._

case class TableDescriptor[A <: ThriftStruct : Manifest : Describe, B: Manifest: TupleSetter](database: String, partition: Partition[A, B]) {

  def createHiveDescriptor(): HiveTableDescriptor = {
    val describe = Describe.of[A]
    val (fields: List[TField]) = describe.metadata.map(x => x._2)
    val columnNames = describe.metadata.map(x => x._1).toArray
    val columnTypes: Array[String] = fields.map(f => mapType(f.`type`)).toArray
    new ParquetTableDescriptor(database, describe.name, columnNames, columnTypes, partition.fieldNames.toArray)
  }

  def tablePath(hiveConf: HiveConf) =
    s"${hiveConf.getVar(HiveConf.ConfVars.METASTOREWAREHOUSE)}/${database}/${name}"

  def name = {Describe.of[A].name}

  def qualifiedName = {database + "." + name}

  def createScheme() = new ParquetScroogeScheme[A]
}
