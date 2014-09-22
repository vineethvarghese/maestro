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

  /** Taste file, containing histogram of what type of data is in the columns. 
   *
   *  Example:
   *  Any:1000, AlphaNum:900,  Real:10;
   *  Any:1000, AlphaNum:1000;
   *  Any:1000, Real:800, Nat:120;
   */
  @Required var taste: String   = _
}


/** Infer a schema, based on the names file and histogram. */
object Infer extends ArgMain[InferArgs]
{
  def main(args: InferArgs): Unit = {

    // Read and parse the taste (histogram) file.
    val strTaste: String =
      Source.fromFile(args.taste)
        .getLines .mkString ("\n")

    val taste: List[Histogram] =
      parser.Taste(strTaste) match {
        case Left(err)  
         => throw new Exception("taste parse error: " ++ err.toString())
        case Right(s) => s.toList
      }

    // Squash all the histograms, so that we retain just the most specific
    // type for each column.
    val tasteSquashed =
      taste.map { h => schema.Squash.squash(h) }

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
}

