package au.com.cba.omnia.maestro.core
package codec

import test.Arbitraries._

import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._

import org.scalacheck._, Arbitrary._

import org.apache.thrift.protocol.TField


object DescribeSpec extends test.Spec { def is = s2"""

Describe Properties
===================

  Describe.describe                                         $isDescription
  Describe.of                                               $isOf

"""

  case class Song(name: String, artist: String)

  def isDescription = prop((n: String, xs: List[(String, TField)]) => {
    implicit def cDesc = Describe[Song](n, xs)
    Describe.describe[Song] must_== xs
  })

  def isOf = prop((n: String, xs: List[(String, TField)]) => {
    implicit def cDesc = Describe[Song](n, xs)
    Describe.of[Song] must_== Describe[Song](n, xs)
  })

}
