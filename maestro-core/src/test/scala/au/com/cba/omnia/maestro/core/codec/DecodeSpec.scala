package au.com.cba.omnia.maestro.core
package codec

import test.Arbitraries._
import au.com.cba.omnia.maestro.core.data._

import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._

import org.scalacheck._, Arbitrary._


object DecodeSpec extends test.Spec { def is = s2"""

Decode laws
===========

  lawful monad                                             ${monad.laws[Decode]}

Decode properties
=================

  boolean decoding                                          ${primitive[Boolean](_.toString)}
  int decoding                                              ${primitive[Int](_.toString)}
  long decoding                                             ${primitive[Long](_.toString)}
  double decoding                                           ${primitive[Double](_.toString)}
  string decoding                                           ${primitive[String](_.toString)}
  tuple decoding                                            ${tuples}

Decode witness
==============

  If this spec compiles then we now we know we can derive primitive and product Decode instances  ${ok}

"""

  def primitive[A: Arbitrary: Decode: Encode](toString: A => String) = prop((v: A) => {
    (Decode.decode(ValDecodeSource(Encode.encode(v))) must_== DecodeOk(v)) and
      (Decode.decode(UnknownDecodeSource(toString(v) :: Nil)) must_== DecodeOk(v)) })

  def tuples = prop((b: Boolean, i: Int, l: Long, d: Double, s: String) => {
    val tuple = (b, i, l, d, s, (b, i, l, d, s))
    Decode.decode[(Boolean, Int, Long, Double, String, (Boolean, Int, Long, Double, String))](
        ValDecodeSource(Encode.encode(tuple))) must_== DecodeOk(tuple) })

  /* witness these codecs, compilation is the test */

  Decode.of[String]
  Decode.of[Int]
  Decode.of[Long]
  Decode.of[Double]
  Decode.of[Boolean]
  Decode.of[List[Int]]
  Decode.of[Vector[Int]]
  Decode.of[(String, Int)]
  Decode.of[(String, (Long, Int), (Double, Boolean))]
  Decode.of[Example]

  case class Nested(i: Int, b: Boolean)
  case class Example(s: String, l: Long, n: Nested)

  implicit def DecodeIntEqual: Equal[Decode[Int]] =
    Equal.equal((a, b) => {
      val starting = (1 to 100).toList
      val unknown = UnknownDecodeSource(starting.map(_.toString))
      val known = ValDecodeSource(starting.map(IntVal))
      (a.run(unknown, 0) == a.run(unknown, 0)) &&
        (a.run(known, 0) == b.run(known, 0))
    })
}
