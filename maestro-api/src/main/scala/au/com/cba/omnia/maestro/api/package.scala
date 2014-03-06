package au.com.cba.omnia.maestro

package object api {
  val Partition = au.com.cba.omnia.maestro.core.partition.Partition
  val Guard = au.com.cba.omnia.maestro.core.hdfs.Guard
  type Cascade = au.com.cba.omnia.maestro.core.scalding.Cascade
}
