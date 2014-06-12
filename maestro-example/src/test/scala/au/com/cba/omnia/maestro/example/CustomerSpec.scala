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
