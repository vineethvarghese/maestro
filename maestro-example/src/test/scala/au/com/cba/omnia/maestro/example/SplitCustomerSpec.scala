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

import scalaz.Scalaz._
import scalaz.scalacheck.ScalaCheckBinding._

import au.com.cba.omnia.thermometer.core.{ThermometerSpec, Thermometer}, Thermometer._
import au.com.cba.omnia.thermometer.fact.PathFactoids._

import au.com.cba.omnia.ebenezer.test.ParquetThermometerRecordReader

import au.com.cba.omnia.maestro.core.codec._
import au.com.cba.omnia.maestro.macros._
import au.com.cba.omnia.maestro.test.Records
import au.com.cba.omnia.maestro.example.thrift.Customer

class SplitCustomerSpec extends ThermometerSpec with MacroSupport[Customer] with Records { def is = s2"""

SplitCustomer Cascade
=====================

  end to end pipeline   $facts

"""

  // Because this is a different customer to the one in maestro-test
  lazy val decoder = Macros.mkDecode[Customer]
  def actualReader = ParquetThermometerRecordReader[Customer]
  def expectedReader = delimitedThermometerRecordReader[Customer]('|', decoder)

  def facts = withEnvironment(path(getClass.getResource("environment").toString)) {
    val cascade = withArgs(Map("env" -> s"$dir/user"))(new SplitCustomerCascade(_))
    cascade.withFacts(
      path(cascade.catView)  ==> recordsByDirectory(actualReader, expectedReader, "split-expected" </> "by-cat"),
      path(cascade.dateView) ==> recordsByDirectory(actualReader, expectedReader, "split-expected" </> "by-date")
    )
  }

}
