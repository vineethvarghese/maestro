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

import au.com.cba.omnia.maestro.schema.hive._
import au.com.cba.omnia.maestro.schema.syntax._
import au.com.cba.omnia.maestro.schema.pretty._


/** Table Specification.
 *
 *  @param database    database name
 *  @param table       table name
 *  @param ignore      defines what rows to ignore in the input, eg for header fields
 *  @param columnSpecs specifications for each column
 */
case class TableSpec(
  database:    String,
  table:       String,
  ignore:      Seq[Schema.Ignore],
  columnSpecs: Seq[ColumnSpec]) {

  /** Pretty print a TableSpec as a String */
  def pretty: String =
    columnSpecs .map (c => c.pretty)
      .mkString("\n")
}


/** Column specification.
 *
 *  @param name      column name
 *  @param hivetype  hive storage type (eg String)
 *  @param format    data format / semantic type for the column
 *  @param histogram histogram of how many values match each classifier
 *  @param comment   comment associated with the column
 */
case class ColumnSpec(
  name:        String,
  hivetype:    HiveType.HiveType,
  format:      Option[Format],
  histogram:   Histogram,
  comment:     String) {

  /** Check if this value matches the column format. */
  def matches(s: String): Boolean =
    format match {
      case None     => true
      case Some(f)  => f.matches(s) 
    }


  /** Pretty print the ColumnSpec as a String. */
  def pretty: String = 
    f"$name%-25s"               + " | "  + 
    HiveType.pretty(hivetype)   + " | "  + 
    prettyOptionFormat(format)  + "\n"   + 
    " " * 25                    + " | "  + 
    histogram.pretty + ";\n"


  /** Pretty print a Format as String, or '-' for a missing format. */
  def prettyOptionFormat(of: Option[Format]): String = 
    of match { 
      case None     => "-"
      case Some(f)  => f.pretty 
    }
}


/** Data format / semantic type of a column.
 *  @param list All data values must match one of these classifiers.
 */
case class Format(list: List[Classifier]) {

  /** Append two formats. */
  def ++(that: Format): Format = 
    Format(this.list ++ that.list)

  /** Check if this string matches any of the classifiers in the Format. */
  def matches(s: String): Boolean = 
    list.exists(_.matches(s))

  /** Pretty print a Format as a string. */
  def pretty: String =
    list 
      .map {_.name}
      .mkString (" + ")
}


/** Histogram of how many values in a column match each Classifier 
 *
 *  @param counts How many values match each classifier.
 */
case class Histogram(counts: Map[Classifier, Int]) {

  /** Yield the number of classifiers in the histogram. */
  def size: Int =
    this.counts.size


  /** Pretty print the histogram as a String. */
  def pretty: String = {

    val strs =
      sorted
        // Pretty print with the count after the classifier name.
        .map      { case (c, i)   => c.name + ":" + i.toString }

    // If there aren't any classifications for this field then
    // just print "-" as a placeholder.
    if (strs.length == 0) "-"
    else strs.mkString(", ")
  }


  /** Convert the histogram to JSON. */
  def toJson: JsonDoc =
    JsonMap(
      sorted
        .map  { case (c, i) => (c.name, JsonNum(i)) })


  /** Extract the histogram with the classifiers in the standard sorted
      order. Don't return classifiers with zero counts. */
  def sorted: Seq[(Classifier, Int)] = 
    counts
      .toSeq

      // Drop classifications that didn't match.
      .filter   { case (c, i)   => i > 0 }

      // Sort classifications so we get a deterministic ordering
      // between runs.
      .sortWith { (x1, x2) => x1._1.sortOrder < x2._1.sortOrder }
}


/** Companion object for Schemas. */
object Schema {

  /** Base trait for column entries that are ignored. */
  sealed trait Ignore

  /**
   * Ignore entries in a field that are equal to some value.
   *
   * Encoded as
   * {{{
   *   name = "VALUE"
   * }}}
   * in the text format.
   *
   *
   * @param field field to check
   * @param value value to ignore in this field
   */
  case class IgnoreEq(field: String, value: String) extends Ignore

  /**
   * Ignore entries in a field that are equal to null.
   *
   * Encoded as
   * {{{
   *   name is null
   * }}}
   * in the text format.
   *
   * @param field field to ignore when it is null
   */
  case class IgnoreNull(field: String) extends Ignore


  /** Show the classification counts for row of the table.
   *  The counts for each field go on their own line. */
  def showCountsRow(
    classifications: Array[Classifier],
    counts:          Array[Array[Int]])
    : String =
    counts
      .map { counts => showCountsField(classifications, counts) + ";\n" }
      .mkString


  /** Show the classification counts for a single field,
   *  or '-' if there aren't any. */
  def showCountsField(
    classifications: Array[Classifier],
    counts:          Array[Int])
    : String =
    Histogram(Classifier.all .zip (counts) .toMap).pretty


  def toJsonCountsField(
    classifications: Array[Classifier],
    counts:          Array[Int])
    : String =
    Histogram(Classifier.all .zip (counts) .toMap).pretty
}

