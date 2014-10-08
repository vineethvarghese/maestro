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

import scala.io.Source
import org.apache.hadoop.fs._
import org.apache.commons.lang.StringEscapeUtils._
import com.twitter.scalding._, Dsl._, TDsl._

import au.com.cba.omnia.maestro.schema._
import au.com.cba.omnia.maestro.schema.hive.Input


/** Job to check a table against an existing schema. 
 *  Rows that do not match the schema get written to the given errors file. */
class Check(args: Args) 
  extends Job(args) {

  // Local file containing schema to check against.
  val fileSchema  = args("in-schema")

  // Source HDFS path for input data.
  val filesInput  = Input.list(args("in-hdfs-data"))
  val pipeInput: TypedPipe[String] =
    TypedPipe.from(MultipleTextLineFiles(filesInput.map{_.toString} :_*))

  // Output HDFS path for rows that do not match the schema.
  val pipeOutErrors  = TypedTsv[String](args("out-hdfs-errors"))

  // Output HDFS path for error diagnosis
  val pipeOutDiag    = TypedTsv[String](args("out-hdfs-diag"))


  // Load column names and formats from the schema definition.
  val namesFormats: List[Check.ColumnDef] = 
    Load.loadFormats(fileSchema)

  // Check the input files.
  val pipeErrors =
    pipeInput

     // Check each row against the schema, producing a sequence of fields
     // that don't match.
    .map      { s: String =>
      Check.validRow(namesFormats, '|', s).toSeq }

     // Only report on rows that have at least one error.
    .filter   { r: Seq[(Check.ColumnDef, String)] =>
      r.size > 0 }

  // Write error rows to HDFS
  pipeErrors
    .map   { r => Pretty.prettyRow(r) }
    .write (pipeOutErrors)

  // Write error diagnosis for error rows to HDFS
  pipeErrors
    .map   { r => Pretty.prettyDiag(r) }
    .write (pipeOutDiag)
}


object Check {

  /** A column name along with its format. */
  type ColumnDef 
    = (String, Option[Format])


  /** Check if a row matches the associated table spec.
   *  Returns the column specs and field values that don't match. */
  def validRow(columnDefs: List[ColumnDef], separator: Char, s: String)
    : List[(ColumnDef, String)] =
    s .split(separator)
      .zip     (columnDefs)
      .flatMap { case (field, columnDef) => checkField(columnDef, field) }
      .toList


  /** Check if a field value matches its associated column spec,
   *  returning None if yes, or the original spec and string if no. */
  def checkField(columnDef: ColumnDef, s: String)
    : Option[(ColumnDef, String)] =
    columnDef._2 match {
      case None          => None
      case Some(format)  => 
        if (format.matches(s))
              None
        else  Some((columnDef, s)) }
}


object Pretty {
  
  /** Pretty print an optional format, showing '-' for the None case. */
  def prettyOptionFormat(op: Option[Format]): String = 
    op match { 
        case None    => "-"
        case Some(f) => f.pretty 
    }


  /** Pretty print a row as TSV. */
  def prettyRow (row: Seq[(Check.ColumnDef, String)]): String =
    row
      .map(_._2)
      .mkString("\n")


  /** Pretty print a sequence of check errors. */
  def prettyDiag(errors: Seq[(Check.ColumnDef, String)]): String = 
    errors
      .map { case (columnDef, s) =>
        columnDef._1 ++ 
        "("       ++ 
        "\"" ++ escapeJava(s) ++ "\"" ++ 
        " : "     ++ 
        prettyOptionFormat(columnDef._2) ++ 
        ")" 
       }
      .mkString("; ")
}

