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

object SplitMacroSpec extends Spec { def is = s2"""

SplitMacro
===========

  Take a thrift struct and split it into several smaller structs, duplicating and rearranging the fields  $split
  Take a large thrift struct and split it $splitLarge
"""

  val splitter      = Macros.split[Types, (SubOne, SubTwo, SubThree)]
  val largeSplitter = Macros.split[Large, (SubOne, SubThree, SubFour)]

  def split = prop { (types: Types) =>
    val s1 = new SubOne()
    s1.stringField  = types.stringField
    s1.booleanField = types.booleanField

    val s2 = new SubTwo()
    s2.intField    = types.intField
    s2.longField   = types.longField
    s2.doubleField = types.doubleField

    val s3 = new SubThree()
    s3.longField = types.longField
    s3.intField  = types.intField

    splitter(types) must_== ((s1, s2, s3))
  }

  def splitLarge = {
    val large = createLarge

    val s1 = new SubOne()
    s1.stringField  = large.stringField
    s1.booleanField = large.booleanField

    val s3 = new SubThree()
    s3.longField = large.longField
    s3.intField  = large.intField

    val s4 = new SubFour()
    s4.field1000 = large.field1000
    s4.field10   = large.field10
    s4.field100  = large.field100

    largeSplitter(large) must_== ((s1, s3, s4))
  }

  // Should fail
  // Can't create split for au.com.cba.omnia.maestro.test.thrift.humbug.Types to (Int, String): Int, String are not ThriftStructs
  //val invalid = Macros.split[Types, (Int, String)]
  //Can't create split for au.com.cba.omnia.maestro.test.thrift.humbug.Types to (au.com.cba.omnia.maestro.test.thrift.humbug.SubOne, au.com.cba.omnia.maestro.test.thrift.humbug.Unsub): au.com.cba.omnia.maestro.test.thrift.humbug.Types does not have otherStringField: String
  //val invalid2 = Macros.split[Types, (SubOne, Unsub)]
}
