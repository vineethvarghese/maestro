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

package au.com.cba.omnia.maestro.core.filter

import org.scalacheck._, Arbitrary._

import au.com.cba.omnia.maestro.core.test.Spec

object RowFilterSpec extends Spec { def is = s2"""

RowFilter
=========

  can create a pass through filter                  $keep
  can only keep elements with the right first value $byRowLeader
  can drop the last element                         $init
  can combine filters                               $and
"""

  def keep = prop { (l: List[String]) =>
    RowFilter.keep.run(l) must_== Option(l)
  }

  def byRowLeader =  prop { (t: Boolean, l: List[String]) =>
    val row    = t.toString :: l
    val filter = RowFilter.byRowLeader("true")

    if (t) filter.run(row) must_== Option(l)
    else   filter.run(row) must_== None
  }

  def init = prop { (l: List[String]) =>
    val result = RowFilter.init.run(l)
    if (!l.isEmpty) result must_== Option(l.init)
    else           result must beNone
  }

  def and = prop { (t1: Boolean, t2: Boolean, l: List[String]) =>
    val row = t1.toString :: t2.toString :: l
    val filter = RowFilter.byRowLeader("true") &&& RowFilter.byRowLeader("true")

    if (t1 && t2) filter.run(row) must_== Option(l)
    else          filter.run(row) must beNone
  }
}
