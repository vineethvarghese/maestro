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

package au.com.cba.omnia.maestro.macros

import scalaz._, Scalaz._

import au.com.cba.omnia.maestro.core.codec._
import au.com.cba.omnia.maestro.core.data.StringVal

import au.com.cba.omnia.maestro.test.Spec
import au.com.cba.omnia.maestro.test.Arbitraries._
import au.com.cba.omnia.maestro.test.thrift.scrooge.Types

object ScroogeDecodeMacroSpec extends Spec { def is = s2"""

ScroogeDecodeMacro
===========

  decode from UnknownDecodeSource       $unknown
  decode from ValDecodeSource           $valdecode
  type mismatch ValDecodeSource         $typeErrorVal
  type mismatch UnknownDecodeSource     $typeErrorUnknown
  not enough fields ValDecodeSource     $sizeErrorVal
  not enough fields UnknownDecodeSource $sizeErrorUnknown

"""

  implicit val encodeTypes = Macros.mkEncode[Types]
  implicit val decodeTypes = Macros.mkDecode[Types]

  def valdecode = prop { (types: Types) =>
    decodeTypes.decode(ValDecodeSource(Encode.encode(types))) must_== DecodeOk(types)
  }

  def unknown = prop { (types: Types) =>
    val expected = types.copy(optStringField = Option(types.optStringField.getOrElse("")))
    val unknown = UnknownDecodeSource(List(
      expected.stringField,
      expected.booleanField.toString,
      expected.intField.toString,
      expected.longField.toString,
      expected.doubleField.toString,
      expected.optIntField.cata(_.toString, ""),
      expected.optStringField.get
    ))

    decodeTypes.decode(unknown) must_== DecodeOk(expected)
  }

  def typeErrorVal = {
    val encoded = ValDecodeSource(List(
      StringVal("1"), StringVal("2"), StringVal("3"), StringVal("4"), StringVal("5"),
      StringVal("6"), StringVal("7")
    ))
    decodeTypes.decode(encoded) must_== DecodeError(
      ValDecodeSource(List(StringVal("2"), StringVal("3"), StringVal("4"), StringVal("5"), StringVal("6"), StringVal("7"))),
      1,
      ValTypeMismatch(StringVal("2"), "Boolean")
    )
  }

  def sizeErrorVal = {
    decodeTypes.decode(ValDecodeSource(List())) must_== DecodeError(
      ValDecodeSource(List()),
      0,
      NotEnoughInput(7, "au.com.cba.omnia.maestro.test.thrift.scrooge.Types")
    )
  }

  def typeErrorUnknown = {
    val encoded = UnknownDecodeSource(List(
      "1", "2", "3", "4", "5", "6", "7"
    ))
    decodeTypes.decode(encoded) must parseErrorMatcher[Types](DecodeError(
      UnknownDecodeSource(List("2", "3", "4", "5", "6", "7")),
      1,
      ParseError("2", "Boolean", null)
    ))
  }

  def sizeErrorUnknown = {
    decodeTypes.decode(UnknownDecodeSource(List())) must_== DecodeError(
      UnknownDecodeSource(List()),
      0,
      NotEnoughInput(7, "au.com.cba.omnia.maestro.test.thrift.scrooge.Types")
    )
  }

  def parseErrorMatcher[T] = (be_==(_: DecodeResult[T])) ^^^ ((_: DecodeResult[T]) match {
    case e@DecodeError(_, _, r@ParseError(_, _, _)) =>
      e.copy(reason = r.copy(error = null))
    case other => other
  })
}
