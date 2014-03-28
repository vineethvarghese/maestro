package au.com.cba.omnia.maestro.core
package codec

import test.Arbitraries._

import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._

import org.scalacheck._, Arbitrary._

import data.Field


object TagSpec extends test.Spec { def is = s2"""

Tag Properties
==============

  Tag.tag                                                   $isTag
  Tag.of                                                    $isOf

"""

  case class Song(name: String, artist: Int)
  lazy val t1 = (x: Song) => ""
  lazy val t2 = (x: Song) => 1
  def f1(n: String) = Field[Song, String](n, t1)
  def f2(n: String) = Field[Song, Int](n, t2)

  def isTag = prop((xs: List[String]) => xs.length >= 2 ==> {
    implicit def cTag = Tag[Song](_.zip(List(f1(xs(0)), f2(xs(1)))))
    Tag.tag[Song](xs) must_== List((xs(0), f1(xs(0))), (xs(1), f2(xs(1))))
  })

  def isOf = prop((xs: List[String]) => xs.length >= 2 ==> {
    implicit def cTag = Tag[Song](_.zip(List(f1(xs(0)), f2(xs(1)))))
    Tag.of[Song].run(xs) must_== Tag[Song](_.zip(List(f1(xs(0)), f2(xs(1))))).run(xs)
  })

}
