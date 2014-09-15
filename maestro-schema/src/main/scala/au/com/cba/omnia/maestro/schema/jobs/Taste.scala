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
package jobs

import scala.collection._
import org.apache.hadoop.fs._
import com.twitter.scalding._
import TDsl._

import au.com.cba.omnia.maestro.schema._
import au.com.cba.omnia.maestro.schema.taste._
import au.com.cba.omnia.maestro.schema.hive.Input


/** Job to scan through data from a input table and build a histogram
  * of how many values in each column match each possible classifier. */
class Taste(args: Args) 
  extends Job(args) 
{
  val nameInput    = args("input")
  val fileOutput   = args("output")

  // Get the list of files under this path.
  val filesInput  = Input.list(nameInput)
  val pipeInput   = MultipleTextLineFiles(filesInput.map{_.toString} :_*)
  val pipeOutput  = TextLine(fileOutput)

  // (thread local)
  // Holds the counts of how many values match each classifier.
  // Each mapper job updates its own mutable map with the classifier counts
  // gained from its chunk of data. The maps from each job are then combined
  // in a single reducer.
  val taste: TableTaste
    = TableTaste.empty(100)

  // Read input files.
  pipeInput.read.typed ('line -> 'l) { tpipe: TypedPipe[String] =>

    tpipe

      // Build a map of which types are matched by each line.
      // The first time the worker is called it passes on a reference to the
      // mutable counts map. For each successive time, it destructively accumulates
      // the new counts into the existing map.
      .flatMap { line: String => 
        TableTaste.accumulateRow(taste, line) }

      // Sum up the counts from each node on a single reducer.
      .groupAll
      .reduce (TableTaste.combine)
      .values

      // Show the classifications in a human readable format.
      // TODO: hacks, we're only returning counts for the rows with the
      // most fields. This won't work if the nodes get rows with diff number of fields.
      // We need to combine counts for rows with the same number of fields.
      .map { tasteFinal : TableTaste => {

        val ccounts   = tasteFinal.rowTastes
        val lcounts   = ccounts.toList
        val maxField  = lcounts .map { _._1 } .max
        val hist      = ccounts(maxField).fieldTastes.map { _.clasCounts }
        Schema.showCountsRow (Classifier.all, hist) } }

      // Write the counts out to file.
  } .write (pipeOutput)
}


/*
object Taste 
{ 
  type Counts = mutable.Map[Int, Array[Array[Int]]]


  /** Get all the possible classifications for each field in a PSV. */
  def classifyPSV(s: String): Array[Array[Int]] 
    = s.split('|')
       .map(classifyField)

  // ********* TODO: shift this into TableTaste, and use that instead of Counts
  // ********** THEN add per-field spill histograms to FieldTaste.

  /** Get all the possible classifications for each field in a PSV. */
  def classifyPSVmut(counts: Counts, s: String)
    : Option[Counts] = {

    // Remember whether the initial map was empty.
    // If it is then this is the first row that we're processing,
    // so we want to pass on our (mutable) thread-local map to the reducer.
    val init    = counts.size == 0

    // Split the input row into individual fields.
    val fields  = s.split('|')

    // Get the array that that contains classifier counts for rows with this
    // many fields, and add the classifier counts for this row to it.
    counts.get(fields.length) match {

      // We don't yet have an array of classifier counts for rows with this
      // many fields, so make one now.
      case None      => {

        val ccounts = fields.map(classifyField)
        counts  += ((fields.length, ccounts))
      }

      // We already have an array of classifier counts for rows with this
      // many fields, so acumulate the new counts into it.
      case Some(ccounts) => {
        for (f <- 0 to fields.length - 1) {
          for (c <- 0 to Classifier.all.length - 1) {
            val field     = fields(f)
            val clas      = Classifier.all(c)
            val n: Int    = if (clas.likeness(field) >= 1.0) 1 else 0
            
            // Update the existing classifier count.
            ccounts(f)(c) = ccounts(f)(c) + n
          }
        }
      }
    }

    // If this was the first row we've seen on this thread
    // then return our new map of counts to the reducer.
    if(init)  Some(counts)
    else      None
  }


  /** Get all the possible classifications for a given field. */
  def classifyField(s: String): Array[Int] 
    = Classifier.all
        .map {_.likeness(s)}
        .map {like => if (like >= 1.0) 1 else 0}


  def combineRowCountMaps
      (xs1: Counts, xs2: Counts): Counts = {

    for((num, arr) <- xs1) {
      if (xs2.isDefinedAt(num))
            xs2(num) =  combineRowCounts(arr, xs2(num))
      else  xs2      += ((num, arr))
    }

    xs2
  }

  /** Combine the classification counts for two rows. */
  def combineRowCounts
      ( xs1: Array[Array[Int]]
      , xs2: Array[Array[Int]]): Array[Array[Int]]
    = xs1 .zip (xs2) .map { x12 => x12 match { 
        case (x1, x2) => combineFieldCounts(x1, x2) } }


  /** Combine the classification counts for two fields. */
  def combineFieldCounts
      ( xs1: Array[Int]
      , xs2: Array[Int]): Array[Int]
    = xs1 .zip (xs2) .map { x12 => x12 match { 
        case (x1, x2) => (x1 + x2) } }
}
*/

