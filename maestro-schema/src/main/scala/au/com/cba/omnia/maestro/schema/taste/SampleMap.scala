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
package au.com.cba.omnia.maestro.schema
package taste

import scala.collection._

// A SampleMap is used to build a historgram of the number of times we've seen
// each string in a set of strings. The number of strings we're prepared to track
// is limited to some fixed amount.
class SampleMap(maxSize: Int) {

  // Histogram of how many times we've seen each string.
  private val smap: mutable.Map[String, Int]  
    = mutable.Map()

  // Number of strings that we couldn't add to the histogram.
  private var spilled: Int
    = 0

  // Accumulate a string into the historgram.
  def accumulate(str: String) = {
    if (smap .isDefinedAt(str))
      smap += ((str, smap(str) + 1))
    else if (smap.size < maxSize)
      smap +=  ((str, 1))
    else 
      spilled += 1
  }

  // Extract the final historgram, along with how many string values we spilled.
  def extract(): (Map[String, Int], Int)
    = (smap.toMap, spilled)
}