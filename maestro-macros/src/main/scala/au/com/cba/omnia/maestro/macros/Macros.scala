//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package au.com.cba.omnia.maestro.macros

import com.twitter.scrooge._

import au.com.cba.omnia.maestro.core.codec._
import au.com.cba.omnia.maestro.core.transform.Transform

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

  /**
    * Macro to automatically derive Transformations from one thrift struct to another.
    * 
    * It can take a variable number of rules that determine how to calculate the value for the
    * specified target field. Target fields that don't have specified rules are copied from a source
    * field with the same name. If there are no source fields with the same name, and no explicit
    * transformation exists to populate the target field, then the compilation will fail.
    * 
    * Example:
    * {{{
    * val transform = Macros.mkTransform[Types, SubTwo](
    *   ('intField,  (x: Types) => x.intField + 1),
    *   ('longField, (x: Types) => x.longField - 1)
    * )
    * val result: TypedPipe[SubTwo] = pipe.map(transform.run(_))
    * }}}
    */
  def mkTransform[A <: ThriftStruct, B <: ThriftStruct](
    transformations: (Symbol, A => _)*
  ): Transform[A, B] = macro TransformMacro.impl[A, B]
}

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
