package com.cba.omnia.maestro.example

import test.Arbitraries._
import com.cba.omnia.maestro.core.codec._
import com.cba.omnia.maestro.macros._
import com.cba.omnia.maestro.example.thrift._
import scalaz._, Scalaz._

object CustomerSpec extends test.Spec { def is = s2"""

Customer properties
=================

  encode / decode       $codec

"""
  def codec = prop((c: Customer) =>
    Decode.decode[Customer](ValDecodeSource(Encode.encode(c))) must_== DecodeOk(c))
}
