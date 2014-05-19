package au.com.cba.omnia.maestro

package object api {
  type Partition[A, B] = au.com.cba.omnia.maestro.core.partition.Partition[A, B]
  type Clean           = au.com.cba.omnia.maestro.core.clean.Clean
  type Validator[A]    = au.com.cba.omnia.maestro.core.validate.Validator[A]
  type RowFilter       = au.com.cba.omnia.maestro.core.filter.RowFilter
  type Cascade         = au.com.cba.omnia.maestro.core.scalding.Cascade

  val Partition     = au.com.cba.omnia.maestro.core.partition.Partition
  val HivePartition = au.com.cba.omnia.maestro.core.partition.HivePartition
  val Clean         = au.com.cba.omnia.maestro.core.clean.Clean
  val Validator     = au.com.cba.omnia.maestro.core.validate.Validator
  val Check         = au.com.cba.omnia.maestro.core.validate.Check
  val RowFilter     = au.com.cba.omnia.maestro.core.filter.RowFilter
  val Guard         = au.com.cba.omnia.maestro.core.hdfs.Guard
}
