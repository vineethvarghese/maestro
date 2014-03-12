package au.com.cba.omnia.maestro.core
package validate

import test.Arbitraries._
import au.com.cba.omnia.maestro.core.data._

import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._

import org.scalacheck._, Arbitrary._


object ValidatorSpec extends test.Spec { def is = s2"""

Validator properties
====================

  validate on entity                     $entity
  validate on field                      $field
  fail validate                          $fail
  validators compose                     $composition
  validators accumulate error            $accumulation

"""

  case class Person(name: String, age: Int)
  val name = Field[Person, String]("name", _.name)
  val age = Field[Person, Int]("int", _.age)

  def entity = prop((p: Person) => {
    val validator = Validator.by[Person](_.name == p.name, "this shouldn't happen")
    validator.run(p) must_== (p.success) })

  def field = prop((p: Person) => {
    val check = Validator.by[String](_ == p.name, "this shouldn't happen")
    val validator = Validator.of(name, check)
    validator.run(p) must_== (p.success) })

  def fail = prop((p: Person) => {
    val validator = Validator.by[Person](_.name == (p.name + "x"), "this should happen")
    validator.run(p) must_== ("this should happen".failNel) })

  def composition = prop((p: Person) => {
    val validator = Validator.all(
      Validator.by[Person](_.name == p.name, "[name]"),
      Validator.by[Person](_.age == p.age, "[age]")
    )
    validator.run(p) must_== p.success })

  def accumulation = prop((p: Person) => {
    val validator = Validator.all(
      Validator.by[Person](_.name == (p.name + "x"), "[name]"),
      Validator.by[Person](_.age == (p.age + 1), "[age]")
    )
    validator.run(p) must_== NonEmptyList("[name]", "[age]").failure })

  implicit def PersonArbitrary: Arbitrary[Person] =
    Arbitrary(arbitrary[(String, Int)] map Person.tupled)
}
