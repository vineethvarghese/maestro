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
import au.com.cba.omnia.maestro.schema._
import au.com.cba.omnia.maestro.schema.taste._


/** Accumulated taste information for an entire table. */
case class TableTaste(
  maxHistSize: Int,
  rowTastes:   mutable.Map[Int, RowTaste])

/** Functions concerning TableTastes */
object TableTaste {

  /** Create a new, empty TableTaste. */
  def empty(maxHistSize: Int): TableTaste 
    = TableTaste(maxHistSize, mutable.Map())


  /** Destructively accumulate a new row into the TableTaste.
   *  If this was the first row added then also return the taste wrapped in
   *  a Some, otherwise None. */
  def accumulateRow(tt: TableTaste, str: String): Option[TableTaste] = {

    // Remember whether the initial map was empty.
    // If it is then this is the first row that we're processing,
    // so we want to pass on our (mutable) thread-local map to the reducer.
    val init    = tt.rowTastes.size == 0

    // Split the input row into individual fields.
    val fields  = str.split('|')

    // Get the array that that contains classifier counts for rows with this
    // many fields, and add the classifier counts for this row to it.
    tt.rowTastes.get(fields.length) match {

      // We don't yet have an array of classifier counts for rows with this
      // many fields, so make one now.
      case None      => {
        val rowTaste  = RowTaste.classify(tt.maxHistSize, fields)
        tt.rowTastes += ((fields.length, rowTaste))
      }

      // We already have an array of classifier counts for rows with this
      // many fields, so acumulate the new counts into it.
      case Some(rowTaste) => {
        RowTaste.accumulate(rowTaste, fields)
      }

    }

    if(init)  Some(tt)
    else      None
  }


  /** Get all the possible classifications for a given field. */
  def classifyField(s: String): Array[Int] 
    = Classifier.all
        .map {_.likeness(s)}
        .map {like => if (like >= 1.0) 1 else 0}


  /** Combine the information in two TableTastes to produce a new one. */
  def combine(tt1: TableTaste, tt2: TableTaste): TableTaste = {

    val tt3: TableTaste 
      = empty(tt1.maxHistSize)

    for ((num, rt1) <- tt1.rowTastes) {
      if (tt2.rowTastes.isDefinedAt(num))
            tt3.rowTastes += ((num, RowTaste.combine(tt2.rowTastes(num), rt1)))
      else  tt3.rowTastes += ((num, rt1))
    }

    tt3
  }

}