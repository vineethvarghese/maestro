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

import au.com.cba.omnia.thermometer.core.{ThermometerSpec, Thermometer}, Thermometer._
import au.com.cba.omnia.thermometer.fact.PathFactoids._

import au.com.cba.omnia.ebenezer.test.ParquetThermometerRecordReader

import au.com.cba.omnia.maestro.api._
import au.com.cba.omnia.maestro.test.Records
import au.com.cba.omnia.maestro.example.thrift.{Account, Customer}

class TransformCustomerSpec extends ThermometerSpec  with Records { def is = s2"""
TransformCustomer Cascade
=====================

  end to end pipeline   $facts

"""

  lazy val accountDecoder   = Macros.mkDecode[Account]
  def actualAccountReader   = ParquetThermometerRecordReader[Account]
  def expectedAccountReader = delimitedThermometerRecordReader[Account]('|', "null", accountDecoder)

  lazy val customerDecoder   = Macros.mkDecode[Customer]
  def actualCustomerReader   = ParquetThermometerRecordReader[Customer]
  def expectedCustomerReader = delimitedThermometerRecordReader[Customer]('|', "null", customerDecoder)


  def facts = withEnvironment(path(getClass.getResource("/transform-customer").toString)) {
    val cascade = withArgs(Map("env" -> s"$dir/user"))(new TransformCustomerCascade(_))
    cascade.withFacts(
      path(cascade.accountView)  ==> recordsByDirectory(actualAccountReader, expectedAccountReader, "expected" </> "account"),
      path(cascade.customerView) ==> recordsByDirectory(actualCustomerReader, expectedCustomerReader, "expected" </> "customer")
    )
  }
}
