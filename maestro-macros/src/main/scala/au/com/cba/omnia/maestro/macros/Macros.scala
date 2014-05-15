package au.com.cba.omnia.maestro.macros

import com.twitter.scrooge._

import au.com.cba.omnia.maestro.core.codec._

object Macros {
  def mkDecode[A <: ThriftStruct]: Decode[A] =
    macro DecodeMacro.impl[A]

  def mkEncode[A <: ThriftStruct]: Encode[A] =
    macro EncodeMacro.impl[A]

  def mkFields[A <: ThriftStruct]: Any =
    macro FieldsMacro.impl[A]

  def mkTag[A <: ThriftStruct]: Tag[A] =
    macro TagMacro.impl[A]

  def getFields[A <: ThriftStruct]: Any =
    macro FieldsMacro.impl[A]

  def split[A <: ThriftStruct, B <: Product]: A => B =
    macro SplitMacro.impl[A, B]
}

// FIX qualification issue, if codec is not important these don't work as expected
trait MacroSupport[A <: ThriftStruct] {
  implicit def DerivedDecode: Decode[A] =
    macro DecodeMacro.impl[A]

  implicit def DerivedEncode: Encode[A] =
    macro EncodeMacro.impl[A]

  implicit def DerivedTag: Tag[A] =
    macro TagMacro.impl[A]

  /* NOTE: This isn't really any, it is a structural type containing all the fields. */
  def Fields: Any =
    macro FieldsMacro.impl[A]
}
