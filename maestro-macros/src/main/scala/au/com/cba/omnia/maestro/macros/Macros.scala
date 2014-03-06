package au.com.cba.omnia.maestro.macros

import au.com.cba.omnia.maestro.core.codec._
import com.twitter.scrooge._

object Macros {
  def mkDecode[A <: ThriftStruct]: Decode[A] =
    macro DecodeMacro.impl[A]

  def mkEncode[A <: ThriftStruct]: Encode[A] =
    macro EncodeMacro.impl[A]

  def mkFields[A <: ThriftStruct]: Any =
    macro FieldsMacro.impl[A]
}

// FIX qualification issue, if codec is not important these don't work as expected
trait MacroSupport[A <: ThriftStruct] {
  implicit def DerivedDecode: Decode[A] =
    macro DecodeMacro.impl[A]

  implicit def DerivedEncode: Encode[A] =
    macro EncodeMacro.impl[A]

  /* NOTE: This isn't really any, it is a structural type containing all the fields. */
  def Fields: Any =
    macro FieldsMacro.impl[A]
}
