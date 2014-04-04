package au.com.cba.omnia.maestro.example

import test.Arbitraries._
import au.com.cba.omnia.maestro.core.codec._
import au.com.cba.omnia.maestro.macros._
import au.com.cba.omnia.maestro.example.thrift._
import scalaz._, Scalaz._

object CustomerSpec extends test.Spec with MacroSupport[Customer] { def is = s2"""

Customer properties
=================

  encode / decode       $codec

"""

  //TODO: add in a more complete testing example here, using what though??? Thermometer? How do you embed Hive?
  //Fun, fun fun...
  def codec = prop((c: Customer) =>
    Decode.decode[Customer](ValDecodeSource(Encode.encode(c))) must_== DecodeOk(c))
}
