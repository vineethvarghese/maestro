package au.com.cba.omnia.maestro.core
package partition

import au.com.cba.omnia.maestro.core.data._

case class Partition[A, B](fieldNames: List[String], extract: A => B, pattern: String)

object Partition {
  def byFields1[A, B](a: Field[A, B]): Partition[A, B] =
    Partition(List(a.value), a.get, "%s")

  def byFields2[A, B, C](a: Field[A, B], b: Field[A, C]): Partition[A, (B, C)] =
    Partition(List(a.value, b.value), v => (a.get(v), b.get(v)), "%s/%s")

  def byFields3[A, B, C, D](a: Field[A, B], b: Field[A, C], c: Field[A, D]): Partition[A, (B, C, D)] =
    Partition(List(a.value, b.value, c.value),v => (a.get(v), b.get(v), c.get(v)), "%s/%s/%s")

  def byDate[A](date: Field[A, String]): Partition[A, (String, String, String)] =
    Partition(List(date.value), v => date.get(v).split("-").toList match {
      case List(y, m, d) => (y, m, d)
    }, "%s/%s/%s")
}
