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
import au.com.cba.omnia.maestro.schema.pretty._


/** Job to scan through data from a input table and build a histogram
  * of how many values in each column match each possible classifier. */
class Taste(args: Args) 
  extends Job(args) {

  // Get the list of files under this path.
  val nameInput       = args("input")
  val filesInput      = Input.list(nameInput)
  val pipeInput       = MultipleTextLineFiles(filesInput.map{_.toString} :_*)

  // Destination for taste histogram.
  val fileOutputTaste = args("output-taste")
  val pipeOutputTaste = TypedTsv[String](fileOutputTaste)


  // (thread local)
  // Holds the counts of how many values match each classifier.
  // Each mapper job updates its own mutable map with the classifier counts
  // gained from its chunk of data. The maps from each job are then combined
  // in a single reducer.
  val taste: TableTaste
    = TableTaste.empty(100)

  // Read input files.
  pipeInput.read.typed ('line -> 'l) { tpipe: TypedPipe[String] => {

    val pTastes: TypedPipe[TableTaste] =
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
    pTastes
      .map { tasteFinal: TableTaste => 
        JsonDoc.render(0, tasteFinal.toJson) }

      // Write the counts out to file.
      .write(pipeOutputTaste)

  } }
}

