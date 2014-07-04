//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package au.com.cba.omnia.maestro.core
package partition

import au.com.cba.omnia.maestro.core.data._

case class Partition[A, B](fieldNames: List[String], extract: A => B, pattern: String)

object Partition {
  def byField[A, B](a: Field[A, B]): Partition[A, B] =
    Partition(toPartitionName(List(a.name)), a.get, "%s")

  def byFields2[A, B, C](a: Field[A, B], b: Field[A, C]): Partition[A, (B, C)] =
    Partition(toPartitionName(List(a.name, b.name)), v => (a.get(v), b.get(v)), "%s/%s")

  def byFields3[A, B, C, D](a: Field[A, B], b: Field[A, C], c: Field[A, D]): Partition[A, (B, C, D)] =
    Partition(
      toPartitionName(List(a.name, b.name, c.name)),
      v => (a.get(v), b.get(v), c.get(v)),
      "%s/%s/%s"
    )

  def byDate[A](date: Field[A, String]): Partition[A, (String, String, String)] =
    Partition(List("year", "month", "day"), v => date.get(v).split("-").toList match {
      case List(y, m, d) => (y, m, d)
    }, "%s/%s/%s")

  def byHour[A](date: Field[A, String]): Partition[A, (String, String, String, String)] =
    Partition(List("year", "month", "day", "hour"), v => date.get(v).split("-").toList match {
      case List(y, m, d, h) => (y, m, d, h)
    }, "%s/%s/%s/%s")

  /**
    * Prepends `partition_` to column names to avoid clashes between core columns and members of the
    * thrift struct.
    */
  def toPartitionName(columns: List[String]): List[String] = columns.map("partition_" + _)
}

object HivePartition {
  def byField[A, B](a: Field[A, B]): Partition[A, B] =
    Partition(Partition.toPartitionName(List(a.name)), a.get, s"p${a.name}=%s")

  def byFields2[A, B, C](a: Field[A, B], b: Field[A, C]): Partition[A, (B, C)] =
    Partition(
      Partition.toPartitionName(List(a.name, b.name)),
      v => (a.get(v), b.get(v)),
      s"p${a.name}=%s/p${b.name}=%s"
    )

  def byFields3[A, B, C, D](a: Field[A, B], b: Field[A, C], c: Field[A, D]): Partition[A, (B, C, D)] =
    Partition(
      Partition.toPartitionName(List(a.name, b.name, c.name)),
      v => (a.get(v), b.get(v), c.get(v)),
      s"p${a.name}=%s/p${b.name}=%s/p${c.name}=%s"
    )

  def byDate[A](date: Field[A, String]): Partition[A, (String, String, String)] =
    Partition(List("year", "month", "day"), v => date.get(v).split("-").toList match {
      case List(y, m, d) => (y, m, d)
    }, "year=%s/month=%s/day=%s")

  def byHour[A](date: Field[A, String]): Partition[A, (String, String, String, String)] =
    Partition(List("year", "month", "day", "hour"), v => date.get(v).split("-").toList match {
      case List(y, m, d, h) => (y, m, d, h)
    }, "year=%s/month=%s/day=%s/hour=%s")


}

