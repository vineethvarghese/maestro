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

import scala.collection.immutable._

import au.com.cba.omnia.maestro.schema._
import au.com.cba.omnia.maestro.schema.syntax._
import au.com.cba.omnia.maestro.schema.tope._


/** Top-level of the schema inferencer. */
object Infer
{
  /** Given a squashed histogram for a column, 
      try to in infer a suitable format / semantic type for it. */
  def infer(hist: Histogram): Option[Format] = {

    // All the classifiers in the historgram.
    val classifiers
      = hist.counts.toSeq.map { _._1 }

    // All the syntax classifiers.
    val syntaxes
      = classifiers .flatMap { 
          case ClasSyntax(s)  => Seq(s) 
          case ClasTope(_, _) => Seq() }

    // All the tope classifiers.
    val topes
      = classifiers .filter {
          case ClasSyntax(_)  => false
          case ClasTope(_, _) => true }

    // If we have a single classifier that is not Any, then use that.
    // This is the best possible case, as all the data in the column
    // had the same uniform type.
    if (hist.counts.size == 1) {
      val clas  = classifiers(0)
      if (clas != ClasSyntax(Any))
           Some (Format(List(hist.counts.toSeq(0)._1)))
      else None
    }

    // If all classifiers are syntaxes and siblings then we have a
    // union type.
    else if (topes.size == 0 && Syntaxes.areSiblings(syntaxes.toSet))
      Some(Format(syntaxes.map { s => ClasSyntax(s) }.toList))

    // There isn't an obvious format to use for this column.
    else None
  }
}

