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

import com.twitter.scrooge.ThriftStruct

import org.apache.hadoop.fs.Path

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

import scalaz.Scalaz._
import scalaz.effect.IO
import scalaz.scalacheck.ScalaCheckBinding._

import au.com.cba.omnia.ebenezer.scrooge.ParquetScroogeTools
import au.com.cba.omnia.maestro.core.codec._
import au.com.cba.omnia.maestro.example.thrift.Customer
import au.com.cba.omnia.maestro.macros._
import au.com.cba.omnia.maestro.test.Records
import au.com.cba.omnia.thermometer.core._,Thermometer._
import au.com.cba.omnia.thermometer.fact.PathFactoids._

class SplitCustomerSpec extends ThermometerSpec with MacroSupport[Customer] with Records { def is = s2"""

Customer properties
=================

  encode / decode       $codec
  end to end pipeline   $facts

"""

  // Because this is a different customer to the one in maestro-test
  implicit def CustomerArbitrary: Arbitrary[Customer] = Arbitrary((
    arbitrary[String] |@|
    arbitrary[String] |@|
    arbitrary[String] |@|
    arbitrary[String] |@|
    arbitrary[String] |@|
    arbitrary[Int] |@|
    arbitrary[String])(Customer.apply))

  lazy val decoder = Macros.mkDecode[Customer]
  def codec = prop { (c: Customer) =>
    decoder.decode(ValDecodeSource(Encode.encode(c))) must_== DecodeOk(c)

    val unknown = UnknownDecodeSource(List(
      c.id, c.name, c.acct, c.cat,
      c.subCat, c.balance.toString, c.effectiveDate))

    decoder.decode(unknown) must_== DecodeOk(c)
  }

  def actualReader = ParquetThermometerRecordReader[Customer]
  def expectedReader = delimitedThermometerRecordReader[Customer]('|', decoder)

  //user test hadoop user dir as env. 
  lazy val cascade = new SplitCustomerCascade(scaldingArgs + ("env" -> List(".")))

  def facts = withEnvironment(path(getClass.getResource("environment").toString()))({
    cascade.withFacts(
      path(cascade.catView)  ==> recordsByDirectory(actualReader, expectedReader, "split-expected" </> "by-cat"),
      path(cascade.dateView) ==> recordsByDirectory(actualReader, expectedReader, "split-expected" </> "by-date")
    )
  })

}
