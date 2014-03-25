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

  implicit def DerivedTag: Tag[A] =
    macro TagMacro.impl[A]

  implicit def DeriviedDescribe: Describe[A] =
    macro DescribeMacro.impl[A]

  /* NOTE: This isn't really any, it is a structural type containing all the fields. */
  def Fields: Any =
    macro FieldsMacro.impl[A]

}


/**
 * Note, at the moment I have manually created this arity2, this should be improved with macro annotation later
 */
**/
trait MacroSupport2[A <: ThriftStruct, B <: ThriftStruct] {
  implicit def DerivedDecode1: Decode[A] =
  macro DecodeMacro.impl[A]
  implicit def DerivedDecode2: Decode[B] =
  macro DecodeMacro.impl[B]

  implicit def DerivedEncode1: Encode[A] =
  macro EncodeMacro.impl[A]
  implicit def DerivedEncode2: Encode[B] =
  macro EncodeMacro.impl[B]

  implicit def DerivedTag1: Tag[A] =
  macro TagMacro.impl[A]
  implicit def DerivedTag2: Tag[B] =
  macro TagMacro.impl[B]

  implicit def DeriviedDescribe1: Describe[A] =
  macro DescribeMacro.impl[A]
  implicit def DeriviedDescribe2: Describe[B] =
  macro DescribeMacro.impl[B]

  /* NOTE: This isn't really any, it is a structural type containing all the fields. */
  def Fields1: Any =
  macro FieldsMacro.impl[A]
  def Fields2: Any =
  macro FieldsMacro.impl[B]

}
