package au.com.cba.omnia.maestro.test

import scalaz._, Scalaz._, \&/._

import scalaz.scalacheck.ScalazArbitrary._
import scalaz.scalacheck.ScalaCheckBinding._

import org.scalacheck._, Arbitrary._

import au.com.cba.omnia.maestro.test.thrift.scrooge._
import au.com.cba.omnia.maestro.test.thrift.humbug._

object Arbitraries {
  implicit def CustomerArbitrary: Arbitrary[Customer] = Arbitrary((
    arbitrary[String] |@|
    arbitrary[String] |@|
    arbitrary[String] |@|
    arbitrary[String] |@|
    arbitrary[String] |@|
    arbitrary[Int] |@|
    arbitrary[String])(Customer.apply))

  implicit def TypesArbitrary: Arbitrary[Types] = Arbitrary((
    arbitrary[String]  |@|
      arbitrary[Boolean] |@|
      arbitrary[Int]     |@|
      arbitrary[Long]    |@|
      arbitrary[Double]
  ){ case (string, boolean, int, long, double) =>
      val types = new Types()
      types.stringField = string
      types.booleanField = boolean
      types.intField = int
      types.longField = long
      types.doubleField = double

      types
  })
}
