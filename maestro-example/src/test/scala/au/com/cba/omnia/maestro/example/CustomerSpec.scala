package au.com.cba.omnia.maestro.example

import scalaz._, Scalaz._

import au.com.cba.omnia.maestro.core.codec._
import au.com.cba.omnia.maestro.macros._
import au.com.cba.omnia.maestro.api._

import au.com.cba.omnia.maestro.test.Spec
import au.com.cba.omnia.maestro.test.Arbitraries._
import au.com.cba.omnia.maestro.test.thrift.scrooge.Customer

object CustomerSpec extends Spec with MacroSupport[Customer] { def is = s2"""

Customer properties
=================

  encode / decode       $codec

"""

  def codec = prop { (c: Customer) =>
    Decode.decode[Customer](ValDecodeSource(Encode.encode(c))) must_== DecodeOk(c)

    val unknown = UnknownDecodeSource(List(
      c.customerId, c. customerName, c.customerAcct, c.customerCat,
      c.customerSubCat, c.customerBalance.toString, c.effectiveDate
    ))

    Decode.decode[Customer](unknown) must_== DecodeOk(c)
  }
}
