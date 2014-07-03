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

import au.com.cba.omnia.maestro.core.codec._
import au.com.cba.omnia.maestro.core.data._
import au.com.cba.omnia.maestro.macros._

import au.com.cba.omnia.maestro.test.Spec
import au.com.cba.omnia.maestro.test.Arbitraries._
import au.com.cba.omnia.maestro.test.thrift.humbug._
import au.com.cba.omnia.maestro.test.thrift.scrooge._

object FieldsMacroSpec extends Spec { def is = s2"""

FieldsMacro
===========

The fields macro creates fields

  with the name of the thrift field for humbug  $nameHumbug
  with the name of the thrift field for scrooge $nameScrooge
  that can extract a value for humbug           $extractHumbug
  that can extract a value for scrooge          $extractScrooge
"""

  val typesFields    = Macros.mkFields[Types]
  val customerFields = Macros.mkFields[Customer]

  def nameHumbug = {
    typesFields.StringField.value === "stringField"
    typesFields.LongField.value   === "longField"
    typesFields.DoubleField.value === "doubleField"
  }

  def nameScrooge = {
    customerFields.CustomerId.value    === "CUSTOMER_ID"
    customerFields.CustomerCat.value   === "CUSTOMER_CAT"
    customerFields.EffectiveDate.value === "EFFECTIVE_DATE"
  }

  def extractHumbug = prop { (t: Types) =>
    typesFields.StringField.get(t) === t.stringField
    typesFields.LongField.get(t)   === t.longField
    typesFields.DoubleField.get(t) === t.doubleField
  }

  def extractScrooge = prop { (c: Customer) =>
    customerFields.CustomerId.get(c)    === c.customerId
    customerFields.CustomerCat.get(c)   === c.customerCat
    customerFields.EffectiveDate.get(c) === c.effectiveDate
  }
}
