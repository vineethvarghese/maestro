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


/** Job to check a table against an existing schema. */
class Check(args: Args) 
  extends Job(args) {

  // Local FS file containing schema to check against.
  val fileSchema  = args("schema")

  // Source HDFS path for input files.
  val filesInput  = Input.list(args("input"))
  val pipeInput: TypedPipe[String] =
    TypedPipe.from(MultipleTextLineFiles(filesInput.map{_.toString} :_*))

  // Output HDFS path for bad rows.
  val pipeOutput  = TypedTsv[String](args("output"))

  // Read the schema definition.
  val strSchema = 
    Source.fromFile(fileSchema)
      .getLines .mkString("\n")

  // Parse the schema definition.
  val schema = 
    parser.Schema(strSchema) match { 
      case Left(err) => throw new Exception("schema parse error: " + err.toString())
      case Right(s)  => s
    }

  // Check the input files.
  pipeInput

     // Check each row against the schema, producing a sequence of fields
     // that don't match.
    .map      { s: String =>
      Check.validRow(schema, '|', s).toSeq }

     // Only report on rows that have at least one error.
    .filter   { r: Seq[(ColumnSpec, String)] =>
      r.size > 0 }

     // Pretty print the errors for each row.
    .map      { r: Seq[(ColumnSpec, String)] =>
      Pretty.prettyErrors(r) }

    // Write out the bad rows to HDFS.
    .write   (pipeOutput)
}


object Check {

  /** Check if a row matches the associated table spec.
   *  Returns the column specs and field values that don't match. */
  def validRow(spec: TableSpec, separator: Char, s: String)
    : List[(ColumnSpec, String)] =
    s .split(separator)
      .zip     (spec.columnSpecs)
      .flatMap { case (field, cspec) => checkField(cspec, field) }
      .toList


  /** Check if a field value matches its associated column spec,
   *  returning None if yes, or the original spec and string if no. */
  def checkField(spec: ColumnSpec, s: String)
    : Option[(ColumnSpec, String)] =
    if (spec.matches(s))
          None
    else  Some((spec, s))
}


object Pretty {
  
  /** Pretty print an optional format, showing '-' for the None case. */
  def prettyOptionFormat(op: Option[Format]): String = 
    op match { 
        case None    => "-"
        case Some(f) => f.pretty 
    }


  /** Pretty print a sequence of check errors. */
  def prettyErrors(errors: Seq[(ColumnSpec, String)]): String = 
    errors
      .map { case (spec, s) =>
        spec.name ++ 
        "("       ++ 
        "\"" ++ escapeJava(s) ++ "\"" ++ 
        " : "     ++ 
        prettyOptionFormat(spec.format) ++ 
        ")" 
       }
      .mkString("; ")
}

