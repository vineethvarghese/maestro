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
import scalaz.scalacheck.ScalaCheckBinding._

import org.scalacheck.Arbitrary, Arbitrary._

import au.com.cba.omnia.thermometer.core.{ThermometerSpec, Thermometer}, Thermometer._
import au.com.cba.omnia.thermometer.hive.HiveSupport
import au.com.cba.omnia.thermometer.fact.PathFactoids._

import au.com.cba.omnia.ebenezer.test.ParquetThermometerRecordReader

import au.com.cba.omnia.maestro.core.codec._
import au.com.cba.omnia.maestro.macros._
import au.com.cba.omnia.maestro.api._
import au.com.cba.omnia.maestro.test.Records

import au.com.cba.omnia.maestro.example.thrift.Customer

object CustomerSpec extends ThermometerSpec with MacroSupport[Customer] with Records with HiveSupport { def is = s2"""

Customer Cascade
================

  encode/decode round trip   $codec
  end to end pipeline        $pipeline

"""

  implicit def CustomerArbitrary: Arbitrary[Customer] = Arbitrary((
    arbitrary[String] |@|
    arbitrary[String] |@|
    arbitrary[String] |@|
    arbitrary[String] |@|
    arbitrary[String] |@|
    arbitrary[Int]    |@|
    arbitrary[String]
  )(Customer.apply))

  lazy val decoder = Macros.mkDecode[Customer]

  def codec = prop { (c: Customer) =>
    decoder.decode(ValDecodeSource(Encode.encode(c))) must_== DecodeOk(c)

    val unknown = UnknownDecodeSource(List(
      c.id, c.name, c.acct, c.cat,
      c.subCat, c.balance.toString, c.effectiveDate))

    decoder.decode(unknown) must_== DecodeOk(c)
  }

  def pipeline = {
    val actualReader   = ParquetThermometerRecordReader[Customer]
    val expectedReader = delimitedThermometerRecordReader[Customer]('|', decoder)

    withEnvironment(path(getClass.getResource("environment").toString)) {
      val cascade = withArgs(Map("env" -> s"$dir/user"))(new CustomerCascade(_))
      cascade.withFacts(
        hiveWarehouse </> "customer.db" </> "by_date" ==> recordsByDirectory(actualReader, expectedReader, "customer-expected" </> "by-date"),
        hiveWarehouse </> "customer.db" </> "by_cat"   ==> recordsByDirectory(actualReader, expectedReader, "customer-expected" </> "by-cat")
      )
    }
  }
}
