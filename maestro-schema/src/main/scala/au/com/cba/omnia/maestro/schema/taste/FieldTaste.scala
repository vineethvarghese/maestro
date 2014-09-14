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

// Classifier counts for a single field in a row.
case class FieldTaste(
  clasCounts: Array[Int],
  histField:  SampleMap)


object FieldTaste {

  // Create a new, empty FieldTaste.
  def empty(maxHistSize: Int): FieldTaste = {
    val clasCounts: Array[Int]
      = Classifier.all.map { _ => 0 }

    val histField: SampleMap
      = new SampleMap(maxHistSize)

    FieldTaste(clasCounts, histField)
  } 


  // Given a FieldTaste, and a new field value, destructively update
  // the counters in the FieldTaste.
  def accumulate(ft: FieldTaste, str: String) =
    for (c <- 0 to Classifier.all.length - 1) {
      val clas    = Classifier.all(c)
      val n : Int = if (clas.likeness(str) >= 1.0) 1 else 0
      ft.clasCounts(c) += ft.clasCounts(c) + n
    }


  // Combine the information in two FieldTastes to produce a new one.
  def combine(ft1: FieldTaste, ft2: FieldTaste): FieldTaste = {

    val clasCountsX =
      ft1.clasCounts .zip (ft2.clasCounts) .map { 
        case (x1, x2) => x1 + x2 }

    // TODO: combine histFields

    FieldTaste(clasCountsX, ft1.histField)
  }
}
