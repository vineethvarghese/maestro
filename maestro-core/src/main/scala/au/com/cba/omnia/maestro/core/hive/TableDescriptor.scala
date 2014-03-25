package au.com.cba.omnia.maestro.core.hive

import au.com.cba.omnia.maestro.core.partition.Partition
import org.apache.thrift.protocol.TType
import com.twitter.scrooge.ThriftStruct
import au.com.cba.omnia.maestro.core.codec.Describe
import cascading.tap.hive.{ParquetTableDescriptor, HiveTableDescriptor}
import com.twitter.scalding.TupleSetter
import au.com.cba.omnia.ebenezer.scrooge.ParquetScroogeScheme

//Does this link them properly?
case class TableDescriptor[A <: ThriftStruct : Manifest : Describe, B: Manifest: TupleSetter](database: String, partition: Partition[A, B]) {
  //TODO: complex type handling
  //TODO: find the Hive API that allows me to reference in this mapping, the string mapping here is brittle (I think)
  def mapType(thriftType: Byte) = thriftType match {
    case TType.BOOL => "BOOLEAN"
    case TType.BYTE => "TINYINT"
    case TType.I16 => "SMALLINT"
    case TType.I32 => "INT"
    case TType.I64 => "BIGINT"
    case TType.DOUBLE => "DOUBLE"
    case TType.STRING => "STRING"
    case _ => "STRING"
  }

  def createHiveDescriptor():HiveTableDescriptor = {
    val describe = Describe.of[A]
    val fields = describe.metadata.map(x => x._2)
    val columnNames = fields.map(f => f.name).toArray
    val columnTypes = fields.map(f => mapType(f.`type`)).toArray
    new ParquetTableDescriptor(database, describe.name, columnNames, columnTypes,partition.fieldNames.toArray)
  }

  def tablePath = {
    val n = name
    s"${database}/view/warehouse/${n}"
  }

  def name = {Describe.of[A].name}

  def createScheme() = new ParquetScroogeScheme[A]
}
