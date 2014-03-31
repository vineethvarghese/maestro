package au.com.cba.omnia.maestro.core.hive

import scalaz._, Scalaz._
import au.com.cba.omnia.maestro.core.partition.Partition
import org.apache.thrift.protocol.{TType, TField}
import com.twitter.scrooge.ThriftStruct
import au.com.cba.omnia.maestro.core.codec.Describe
import cascading.tap.hive.{ParquetTableDescriptor, HiveTableDescriptor}
import com.twitter.scalding.TupleSetter
import au.com.cba.omnia.ebenezer.scrooge.ParquetScroogeScheme
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf

case class TableDescriptor[A <: ThriftStruct : Manifest : Describe, B: Manifest: TupleSetter](database: String, partition: Partition[A, B]) {
  // TODO: complex type handling
  def mapType(thriftType: Byte): Option[String] = thriftType match {
    case TType.BOOL => "BOOLEAN".some
    case TType.BYTE => "TINYINT".some
    case TType.I16 => "SMALLINT".some
    case TType.I32 => "INT".some
    case TType.I64 => "BIGINT".some
    case TType.DOUBLE => "DOUBLE".some
    case TType.STRING => "STRING".some
    
    // 1 type param
    case TType.LIST => None
    case TType.SET => None
    case TType.ENUM => None
    
    // 2 type params
    case TType.MAP => None
    
    // n type params
    case TType.STRUCT => None
    
    // terminals
    case TType.VOID => None
    case TType.STOP => None
  }

  def createHiveDescriptor(): HiveTableDescriptor = {
    val describe = Describe.of[A]
    val (fields: List[TField]) = describe.metadata.map(x => x._2)
    // TODO: Use the name from the TField here
    val columnNames = describe.metadata.map(x => x._1).toArray
    val columnTypes = fields.flatMap(f => mapType(f.`type`)).toArray
    new ParquetTableDescriptor(database, describe.name, columnNames, columnTypes, partition.fieldNames.toArray)
  }

  def tablePath = {
    val hiveConf = new HiveConf
    val path = hiveConf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname)
    val n = name
    s"${path}/${database}/${n}"
  }

  def name = {Describe.of[A].name}

  def qualifiedName = {database + "." + name}

  def createScheme() = new ParquetScroogeScheme[A]
}
