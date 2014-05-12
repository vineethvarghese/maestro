package au.com.cba.omnia.maestro.macros

import au.com.cba.omnia.maestro.core.codec._
import au.com.cba.omnia.maestro.core.data._
import au.com.cba.omnia.maestro.macros._

import au.com.cba.omnia.maestro.test.Spec
import au.com.cba.omnia.maestro.test.Arbitraries._
import au.com.cba.omnia.maestro.test.thrift.scrooge._

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

  implicit val encode = Macros.mkEncode[Customer]
  implicit val decode = Macros.mkDecode[Customer]

  Macros.mkTag[Customer]

  def valdecode = prop { (c: Customer) =>
    decode.decode(ValDecodeSource(Encode.encode(c))) must_== DecodeOk(c)
  }

  def unknown = prop { (c: Customer) =>
    val unknown = UnknownDecodeSource(List(
      c.customerId, c. customerName, c.customerAcct, c.customerCat,
      c.customerSubCat, c.customerBalance.toString, c.effectiveDate
    ))

    decode.decode(unknown) must_== DecodeOk(c)
  }

  def typeErrorVal = {
    val encoded = ValDecodeSource(List(
      StringVal("1"), StringVal("2"), StringVal("3"), StringVal("4"), StringVal("5"),
      StringVal("6"), StringVal("7")
    ))
    decode.decode(encoded) must_== DecodeError(
      ValDecodeSource(List(StringVal("6"), StringVal("7"))),
      5,
      ValTypeMismatch(StringVal("6"), "Int")
    )
  }

  def sizeErrorVal = {
    decode.decode(ValDecodeSource(List())) must_== DecodeError(
      ValDecodeSource(List()),
      0,
      NotEnoughInput(7, "au.com.cba.omnia.maestro.test.thrift.scrooge.Customer")
    )
  }

  def typeErrorUnknown = {
    val encoded = UnknownDecodeSource(List(
      "1", "2", "3", "4", "5", "not an int", "7"
    ))
    decode.decode(encoded) must parseErrorMatcher[Customer](DecodeError(
      UnknownDecodeSource(List("not an int", "7")),
      5,
      ParseError("not an int", "Int", null)
    ))
  }

  def sizeErrorUnknown = {
    decode.decode(UnknownDecodeSource(List())) must_== DecodeError(
      UnknownDecodeSource(List()),
      0,
      NotEnoughInput(7, "au.com.cba.omnia.maestro.test.thrift.scrooge.Customer")
    )
  }

  def parseErrorMatcher[T] = (be_==(_: DecodeResult[T])) ^^^ ((_: DecodeResult[T]) match {
    case e@DecodeError(_, _, r@ParseError(_, _, _)) =>
      e.copy(reason = r.copy(error = null))
    case other => other
  })
}
