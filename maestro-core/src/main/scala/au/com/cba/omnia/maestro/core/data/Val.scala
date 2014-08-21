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

package au.com.cba.omnia.maestro.core
package data

import scalaz._, Scalaz._

/* ☠ NOTE: this is called Val to avoid akward name clash with call by Value, please do not change ... ☠ */

sealed trait Val

case class BooleanVal(v: Boolean) extends Val
case class IntVal(v: Int) extends Val
case class LongVal(v: Long) extends Val
case class DoubleVal(v: Double) extends Val
case class StringVal(v: String) extends Val
case object NoneVal extends Val

object Val {
  implicit def ValEqual: Equal[Val] =
    Equal.equalA[Val]
}
