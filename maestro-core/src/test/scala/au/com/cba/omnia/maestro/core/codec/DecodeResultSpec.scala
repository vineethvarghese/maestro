package au.com.cba.omnia.maestro.core
package codec

import test.Arbitraries._
import au.com.cba.omnia.maestro.core.data._

import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._

import org.scalacheck._, Arbitrary._


object DecodeResultSpec extends test.Spec { def is = s2"""

DecodeResult laws
=================

  lawful monad                                             ${monad.laws[DecodeResult]}
  lawful equal                                             ${equal.laws[DecodeResult[Int]]}

"""
}
