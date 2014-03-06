package au.com.cba.omnia.maestro.core
package data

import test.Arbitraries._

import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._

import org.scalacheck._, Arbitrary._


object ValSpec extends test.Spec { def is = s2"""

Val laws
========

  lawful Equal                                              ${equal.laws[Val]}

"""
}
