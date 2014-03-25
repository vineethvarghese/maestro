package au.com.cba.omnia.maestro.core
package hive

import au.com.cba.omnia.maestro.macros.MacroSupport
import au.com.cba.omnia.maestro.core.partition.Partition
import au.com.cba.omnia.maestro.example.thrift._

import org.specs2._


class HiveDescriptorSpec extends Specification with MacroSupport[TestCustomer] { def is = s2"""

Table Descriptor properties
====================

  Generate validate Hive Table Descriptor        $descriptor
  Create table path                              $path
  Derive Table name                              $tableName

"""


  /**
   * struct TestCustomer {
  1  : string CUSTOMER_ID
  2  : string CUSTOMER_NAME
  6  : i32 CUSTOMER_BALANCE
  7  : string EFFECTIVE_DATE
 }
   * */

  def td = TableDescriptor("test", Partition.byDate(Fields.EffectiveDate))

  def descriptor = {
    val descriptor = td.createHiveDescriptor()
    descriptor.getColumnNames must_== Array("CustomerId", "CustomerName", "CustomerBalance", "EffectiveDate")
    descriptor.getColumnTypes must_== Array("STRING", "STRING", "INT", "STRING")
    descriptor.getPartitionKeys must_== Array("EffectiveDate")
  }

  def path = td.tablePath must_== "/hive/warehouse/test/TestCustomer"

  def tableName = td.name must_== "TestCustomer"

}
