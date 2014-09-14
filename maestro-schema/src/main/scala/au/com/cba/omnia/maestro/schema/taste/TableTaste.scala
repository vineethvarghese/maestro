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

/** Accumulated taste information for an entire table. */
case class TableTaste(
  rowTastes: mutable.Map[Int, RowTaste])

/** Functions concerning TableTastes */
object TableTaste {

  // Create a new, empty TableTaste.
  def empty: TableTaste 
    = TableTaste(mutable.Map())

  // Combine the information in two TableTastes to produce a new one.
  def combine(tt1: TableTaste, tt2: TableTaste): TableTaste = {

    val tt3: TableTaste 
      = empty

    for ((num, rt1) <- tt1.rowTastes) {
      if (tt2.rowTastes.isDefinedAt(num))
            tt3.rowTastes += ((num, RowTaste.combine(tt2.rowTastes(num), rt1)))
      else  tt3.rowTastes += ((num, rt1))
    }

    tt3
  }

}