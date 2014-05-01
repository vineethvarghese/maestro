package au.com.cba.omnia.maestro.test

import scalaz._, Scalaz._, \&/._

import scalaz.scalacheck.ScalazArbitrary._
import scalaz.scalacheck.ScalaCheckBinding._

import org.scalacheck._, Arbitrary._

import au.com.cba.omnia.maestro.test.thrift._

object Arbitraries {
  implicit def CustomerArbitrary: Arbitrary[Customer] = Arbitrary((
    arbitrary[String] |@|
    arbitrary[String] |@|
    arbitrary[String] |@|
    arbitrary[String] |@|
    arbitrary[String] |@|
    arbitrary[Int] |@|
    arbitrary[String])(Customer.apply))
}
