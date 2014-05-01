package au.com.cba.omnia.maestro.macros

import au.com.cba.omnia.maestro.core.codec._
import au.com.cba.omnia.maestro.macros._

import au.com.cba.omnia.maestro.test.Spec
import au.com.cba.omnia.maestro.test.Arbitraries._
import au.com.cba.omnia.maestro.test.thrift._

object DecodeMacroSpec extends Spec { def is = s2"""

DecodeMacro
===========

  decode from UnknownDecodeSource $unknown
  decode from ValDecodeSource     $valdecode

"""

  implicit val encode = Macros.mkEncode[Customer]

  def valdecode = prop { (c: Customer) =>
    Macros.mkDecode[Customer].decode(ValDecodeSource(Encode.encode(c))) must_== DecodeOk(c)
  }

  def unknown = prop { (c: Customer) =>
    val unknown = UnknownDecodeSource(List(
      c.customerId, c. customerName, c.customerAcct, c.customerCat,
      c.customerSubCat, c.customerBalance.toString, c.effectiveDate
    ))

    Macros.mkDecode[Customer].decode(unknown) must_== DecodeOk(c)
  }
}
