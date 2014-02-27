package com.cba.omnia.maestro.macros

import com.cba.omnia.maestro.core.codec._
import com.twitter.scrooge._

object Macros {
  def mkDecode[A <: ThriftStruct]: Decode[A] =
    macro DecodeMacro.impl[A]

  def mkEncode[A <: ThriftStruct]: Encode[A] =
    macro EncodeMacro.impl[A]

  def mkFields[A <: ThriftStruct]: Any =
    macro FieldsMacro.impl[A]
}

trait MacroSupport[A <: ThriftStruct] {
}
