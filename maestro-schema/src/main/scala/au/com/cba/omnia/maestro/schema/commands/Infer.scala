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
  @Required var table: String = _

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
  var names: Option[String] = None

  /** Taste file, containing histogram of what type of data is in the columns. */
  @Required var taste: String = _
}


/** Infer a schema, based on the names file and histogram. */
object Infer extends ArgMain[InferArgs]
{
  /** Taster toplevel */
  def main(args: InferArgs): Unit = {

    // Load the names and histograms.
    // Also squash the hisograms so we have just the most specific classifiers.
    val taste = loadTaste(args.names, args.taste)
    val names = taste .map { _._1 }
    val hists = taste .map { _._2 } .map { h => schema.Squash.squash(h) }
        
    // Specs for all the columns.
    val colSpecs = 
      names .zip (hists)
        .map { case (name, hist) => 
            ColumnSpec( 
              name, 
              hive.HiveType.HiveString, 
              schema.Infer.infer(hist), 
              hist,
              "") }

    // Spec for the table.
    val tableSpec: TableSpec =
      TableSpec( 
        args.database,
        args.table,
        List(),
        colSpecs)

    println(JsonDoc.render(0, tableSpec.toJson))
  }  


  /** Load histograms from the taste file at the given path. */
  def loadTaste(fileNames: Option[String], fileTaste: String)
    : (List[(String, Histogram)]) = {

    // Type template for the taste file.
    type JsonTaste
      = Map[String,                 // "row"
            List[Map[String, _]]]   // Meta-data about each column.

    // Read the taste file.
    val strTaste: String =
      Source.fromFile(fileTaste)
        .getLines .mkString ("\n")

    // Do initial JSON parsing of taste file.
    val jsonTaste: JsonTaste = 
      JSON.parseFull(strTaste)
          .getOrElse { throw new Exception("Cannot parse taste file as JSON.") }
          .asInstanceOf[JsonTaste]

    // Get names from the taste file, if there are any.
    val namesTaste: Option[List[String]] =
      (for {
        rows  <- jsonTaste.get("row")
        name  <- sequence(rows.map { row =>
            for { nm <- row.get("name") } 
            yield nm.asInstanceOf[String] })
      } yield name)

    // Get names from the external names file, if we have one.
    val namesExt: Option[List[String]] =
      fileNames.map { file =>
        sequence(Source.fromFile(file)
          .getLines 
          .map       { w  => w  .split("\\s+").toList }
          .map       { ws => ws .lift (0) }
          .toList)
        .getOrElse { throw new Exception("Cannot parse names file.") }
        }

    // Decide where the names come from.
    val names: List[String] = 
      namesExt match {
        case Some(names)   => names
        case None          => namesTaste match {
          case Some(names) => names
          case None        => 
            throw new Exception(
              "No column names in taste file.\n" +
              "and an external names file was not provided.")
          }
        }

    // Get a list of classifier names and counts for each column
    // from the taste file.
    val histsRaw: List[List[(String, Int)]] =
      (for {
        rows  <- jsonTaste.get("row")
        clas  <- sequence(rows.map { row => 
          for { cl <- row.get("classifiers") } 
          yield cl.asInstanceOf[Map[String,_]] })
       } yield clas)
      .getOrElse { throw new Exception("Cannot parse classifiers.") }

      // Truncate incoming classifier counts to integers.
      .map ( m => m.toList.map { case (s, d) => 
        (s, d.asInstanceOf[Double].toInt) })

    // Parse the classifier strings and produce a real histogram for each row.
    val hists: List[Histogram] =
      histsRaw.map { counts => loadHistogram(counts) }

    names .zip (hists)
  }


  /** Load a Histogram from the raw JSON. */
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

