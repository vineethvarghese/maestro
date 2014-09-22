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

import au.com.cba.omnia.maestro.schema.taste._
import au.com.cba.omnia.maestro.schema.pretty._


/** Classifier counts for a row sort, with some fixed number of fields. */
case class RowTaste(
  fieldTastes: Array[FieldTaste]) {

  /* Convert the Row Taste to JSON, attaching field names and storage
   * types if we have them. */
  def toJson(
    fieldNames:   Option[List[String]],
    storageTypes: Option[List[String]]): JsonDoc = 
    JsonMap(
      fieldTastes.toList.zipWithIndex.map { case (ft: FieldTaste, ix: Int) => 
        ("field", ft.toJson(fieldNames.map(_(ix)), storageTypes.map(_(ix))))
      },
      true)
}


object RowTaste {

  /** Create a new, empty RowTaste. */
  def empty(maxHistSize: Int, fieldNum: Int): RowTaste = {

    val fieldTastes: Array[FieldTaste] = 
      (for  { i <- 0 to fieldNum - 1 } 
       yield FieldTaste.empty(maxHistSize))
        .toArray

    RowTaste(fieldTastes)
  }


  /** Compute the possible classifications for the given fields. */
  def classify(maxHistSize: Int, strs: Array[String]): RowTaste =
    RowTaste(strs.map {str => FieldTaste.classify(maxHistSize, str)})


  /** Destructively accumulate field strings into the RowTaste.
   *  If the number of fields is different than the existing RowTaste
   *  then do nothing  */
  def accumulate(rt: RowTaste, strs: Array[String]): Unit = {
    if (rt.fieldTastes.length != strs.length) 
      ()
    else for (f <- 0 to strs.length - 1) {
      FieldTaste.accumulate(rt.fieldTastes(f), strs(f))
    }
  }


  /** Combine the information in two RowTastes to produce a new one. */
  def combine(rt1: RowTaste, rt2: RowTaste): RowTaste = {

    val fieldTastesX: Array[FieldTaste] =
      rt1.fieldTastes .zip (rt2.fieldTastes) .map {
        case (ft1, ft2) => FieldTaste.combine(ft1, ft2) }

    RowTaste(fieldTastesX)
  }
}

