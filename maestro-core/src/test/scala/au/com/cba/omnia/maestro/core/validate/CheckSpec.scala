package au.com.cba.omnia.maestro.core
package validate

import test.Arbitraries._
import au.com.cba.omnia.maestro.core.data._

import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._

import org.scalacheck._, Arbitrary._


object CheckSpec extends test.Spec { def is = s2"""

Check properties
================

  Check.oneOf                 $oneOf
  Check.oneOf failure         $oneOfFailure
  Check.nonempty              $nonempty
  Check.nonempty failure      $nonemptyFailure

"""
  def categories = List("A", "B", "C", "D")
  case class Category(value: String)

  def oneOf = prop((c: Category) =>
    Check.oneOf(categories:_*).run(c.value) must_== c.value.success)

  def oneOfFailure = prop((c: Category) =>
    Check.oneOf(categories:_*).run(c.value + "bad").isFailure)

  def nonempty = prop((s: String) => !s.isEmpty ==> {
    Check.nonempty.run(s) == s.success })

  def nonemptyFailure =
    Check.nonempty.run("").isFailure

  implicit def CategoryArbitrary: Arbitrary[Category] =
    Arbitrary(Gen.oneOf(categories) map Category)

}
