package com.cba.omnia.maestro.example
package test

import com.cba.omnia.maestro.example.thrift._

import scalaz._, Scalaz._, \&/._
import scalaz.scalacheck.ScalazArbitrary._
import scalaz.scalacheck.ScalaCheckBinding._
import org.scalacheck._, Arbitrary._

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
