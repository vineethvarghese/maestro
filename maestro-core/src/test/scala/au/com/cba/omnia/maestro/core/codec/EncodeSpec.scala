package au.com.cba.omnia.maestro.core
package codec

import test.Arbitraries._
import au.com.cba.omnia.maestro.core.data._

import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._

import org.scalacheck._


object EncodeSpec extends test.Spec { def is = s2"""

Encode laws
===========

  lawful contravariant                                      ${contravariant.laws[Encode]}

Encode properties
=================

  boolean encoding                                          ${primitive[Boolean](BooleanVal)}
  int encoding                                              ${primitive[Int](IntVal)}
  long encoding                                             ${primitive[Long](LongVal)}
  double encoding                                           ${primitive[Double](DoubleVal)}
  string encoding                                           ${primitive[String](StringVal)}
  tuple encoding                                            ${tuples}

Encode witness
==============

  If this spec compiles then we now we know we can derive primitive and product Encode instances  ${ok}

"""

  def primitive[A: Arbitrary: Encode](f: A => Val) = prop((v: A) =>
    Encode.encode(v) must_== List(f(v)))

  def tuples = prop((b: Boolean, i: Int, l: Long, d: Double, s: String) =>
    Encode.encode((b, i, l, d, s, (b, i, l, d, s))) must_== twice(List(
        BooleanVal(b)
      , IntVal(i)
      , LongVal(l)
      , DoubleVal(d)
      , StringVal(s)
      )))

  def twice[A](a: List[A]) =
    a ++ a

  /* witness these codecs, compilation is the test */

  Encode.of[String]
  Encode.of[Int]
  Encode.of[Long]
  Encode.of[Double]
  Encode.of[Boolean]
  Encode.of[List[Int]]
  Encode.of[Vector[Int]]
  Encode.of[(String, Int)]
  Encode.of[(String, (Long, Int), (Double, Boolean))]
  Encode.of[Example]

  case class Nested(i: Int, b: Boolean)
  case class Example(s: String, l: Long, n: Nested)

  implicit def EncodeIntEqual: Equal[Encode[Int]] =
    Equal.equal((a, b) => a.run(0) == b.run(0))
}
