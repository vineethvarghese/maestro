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
package codec

import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._

import org.scalacheck._

import au.com.cba.omnia.maestro.core.data._
import au.com.cba.omnia.maestro.core.test.Arbitraries._

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
  option encoding                                           ${option}
  tuple encoding                                            ${tuples}

Encode witness
==============

  If this spec compiles then we now we know we can derive primitive and product Encode instances  ${ok}

"""

  def primitive[A: Arbitrary: Encode](f: A => Val) = prop((v: A) =>
    Encode.encode(v) must_== List(f(v)))

  def option = {
    (Encode.encode(Option(3))         must_== List(IntVal(3))) and
    (Encode.encode(Option.empty[Int]) must_== List(NoneVal))   and
    (Encode.encode(Option("test"))    must_== List(StringVal("test")))
  }

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
  Encode.of[Option[String]]
  Encode.of[Option[Int]]
  Encode.of[Option[Long]]
  Encode.of[Option[Double]]
  Encode.of[Option[Boolean]]

  case class Nested(i: Int, b: Boolean)
  case class Example(s: String, l: Long, n: Nested)

  implicit def EncodeIntEqual: Equal[Encode[Int]] =
    Equal.equal((a, b) => a.run(0) == b.run(0))
}
