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

case class Partition[A, B](extract: A => B, pattern: String)

object Partition {
  def byField[A, B](a: Field[A, B]): Partition[A, B] =
    Partition(a.get, "%s")

  def byFields2[A, B, C](a: Field[A, B], b: Field[A, C]): Partition[A, (B, C)] =
    Partition(v => (a.get(v), b.get(v)), "%s/%s")

  def byFields3[A, B, C, D](a: Field[A, B], b: Field[A, C], c: Field[A, D]): Partition[A, (B, C, D)] =
    Partition(v => (a.get(v), b.get(v), c.get(v)), "%s/%s/%s")

  def byDate[A](date: Field[A, String]): Partition[A, (String, String, String)] =
    Partition(v => date.get(v).split("-").toList match {
      case List(y, m, d) => (y, m, d)
    }, "%s/%s/%s")

  def byHour[A](date: Field[A, String]): Partition[A, (String, String, String, String)] =
    Partition(v => date.get(v).split("-").toList match {
      case List(y, m, d, h) => (y, m, d, h)
    }, "%s/%s/%s/%s")
}

object HivePartition {
  def byField[A, B](a: Field[A, B]): Partition[A, B] =
    Partition(a.get, s"${a.value}=%s")

  def byFields2[A, B, C](a: Field[A, B], b: Field[A, C]): Partition[A, (B, C)] =
    Partition(v => (a.get(v), b.get(v)), s"${a.value}=%s/${b.value}=%s")

  def byFields3[A, B, C, D](a: Field[A, B], b: Field[A, C], c: Field[A, D]): Partition[A, (B, C, D)] =
    Partition(v => (a.get(v), b.get(v), c.get(v)), s"${a.value}=%s/${b.value}=%s/${c.value}=%s")

  def byDate[A](date: Field[A, String]): Partition[A, (String, String, String)] =
    Partition(v => date.get(v).split("-").toList match {
      case List(y, m, d) => (y, m, d)
    }, "year=%s/month=%s/day=%s")

  def byHour[A](date: Field[A, String]): Partition[A, (String, String, String, String)] =
    Partition(v => date.get(v).split("-").toList match {
      case List(y, m, d, h) => (y, m, d, h)
    }, "year=%s/month=%s/day=%s/hour=%s")
}

