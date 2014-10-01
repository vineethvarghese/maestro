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
package commands

import scala.io.Source
import scala.collection._

import com.quantifind.sumac.ArgMain
import com.quantifind.sumac.validation._
import com.quantifind.sumac.FieldArgs

import au.com.cba.omnia.maestro.schema
import au.com.cba.omnia.maestro.schema._
import au.com.cba.omnia.maestro.schema.pretty._
import scala.util.parsing.json.{JSON}


/** Arguments for `Infer` command. .*/
class InferArgs extends FieldArgs {

  /** Name of the database. */
  @Required var database: String = _

  /** Name of the table. */
  @Required var table:    String    = _

  /** File containing names and storage types of columns.
   *  Each line is treated as a description of a single column, 
   *  where the first  word is the column name, 
   *  and   the second word is the hive storage type.
   * 
   *  Example:
   *    paty_i   string
   *    acct_i   string
   *    balance  float
   */
  @Required var columns: String   = _

  /** Taste file, containing histogram of what type of data is in the columns. */
  @Required var taste: String   = _
}


/** Infer a schema, based on the names file and histogram. */
object Infer extends ArgMain[InferArgs]
{
  type JsonTaste
    = Map[String,                              // "row"
          List[Map[String, Map[String, _]]]]   // Meta-data about each column.


  /** Taster toplevel */
  def main(args: InferArgs): Unit = {

    // Read the taste file.
    val strTaste: String =
      Source.fromFile(args.taste)
        .getLines .mkString ("\n")

    // Do initial JSON parsing of taste file.
    val jsonTaste: JsonTaste = 
      JSON.parseFull(strTaste)
          .getOrElse { throw new Exception("Taste file JSON parse error.") }
          .asInstanceOf[JsonTaste]

    // Get maps of classifier name to the value count for each column.
    // This loads directly form the parsed JSON.
    val rawTaste: List[List[(String, Int)]] =
      // Get just the classifiers for each row.
      (for {
        rows  <- jsonTaste.get("row")
        clas  <- sequence(rows.map { row => 
          for { cl <- row.get("classifiers") } yield cl })
       } yield clas)
      .getOrElse { throw new Exception("Cannot get classifiers from taste file") }

      // Truncate incoming classifier counts to integers.
      .map ( m => m.toList.map { case (s, d) => (s, d.asInstanceOf[Double].toInt) })

    // Parse the classifier strings and produce a real histogram for each row.
    val taste: List[Histogram] =
      rawTaste.map { counts => loadHistogram(counts) }

    // Squash all the histograms, so that we retain just the most specific
    // type for each column.
    val tasteSquashed: List[Histogram] =
      taste.map { h => schema.Squash.squash(h) }

    // TODO: get the names from the taste file if they're there,
    //       otherwise allow them to be specified separately.
    val nameTypes: List[List[String]] =
      Source.fromFile(args.columns)
        .getLines .map { w => w.split("\\s+").toList }
        .toList
        
    // Specs for all the columns.
    val colSpecs = 
      nameTypes .zip (tasteSquashed)
        .map { case (nt, h) => 
            ColumnSpec( 
              nt(0), 
              hive.HiveType.HiveString, 
              schema.Infer.infer(h), 
              h,
              "") }

    // Spec for the table.
    val tableSpec: TableSpec =
      TableSpec( 
        args.database,
        args.table,
        Seq(),
        colSpecs)

    println(tableSpec.pretty)
  }  

  // Load a Histogram from the raw JSON.
  def loadHistogram(counts: List[(String, Int)]): Histogram = {
    val clas  = 
      counts.map { case (s, d) => 
        parser.Schema.parseString(parser.Schema.pClassifier, s) match {
          case Left(err) => throw new Exception("Cannot parse classifier: " + s)
          case Right(c)  => (c, d)
        }
      }
    Histogram(clas.toMap)
  }

  /** ZipWith-style helper. */
  def map2[A,B,C](a: Option[A], b: Option[B])(f: (A,B) => C): Option[C] =
    a.flatMap { x => b.map { y => f(x, y) } }

  /** Missing Haskell-esque sequence function. */
  def sequence[A](a: List[Option[A]]): Option[List[A]] =
    a.foldRight[Option[List[A]]](Some(Nil))((x,y) => map2(x,y)(_ :: _)) 
}

