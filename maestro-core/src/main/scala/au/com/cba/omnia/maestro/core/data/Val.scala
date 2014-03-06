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

object Val {
  implicit def ValEqual: Equal[Val] =
    Equal.equalA[Val]
}
