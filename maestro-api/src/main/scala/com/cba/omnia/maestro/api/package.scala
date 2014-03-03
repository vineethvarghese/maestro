package com.cba.omnia.maestro

package object api {
  val Partition = com.cba.omnia.maestro.core.partition.Partition
  val Guard = com.cba.omnia.maestro.core.hdfs.Guard
  type Cascade = com.cba.omnia.maestro.core.scalding.Cascade
}
