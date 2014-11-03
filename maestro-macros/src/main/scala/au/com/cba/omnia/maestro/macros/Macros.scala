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

/**
 * Provides Thrift support for the type [[A]]. Classes extending this trait will get type specific instances of [[Decode]],
 * [[Encode]] and [[Tag]] implicitly. These classes also get access to all the fields for the type via a FieldWrapper obtained
 * when the method Fields is called :-
 * {{{
 * scala> class AccountAware extends MacroSupport[Account] {
 *      |   def balanceField = Fields.Balance
 *      | }
 * defined class AccountAware
 *
 * scala> val a = new AccountAware
 * a: AccountAware = AccountAware@e23edb6
 *
 * scala> a.balanceField
 * res3: au.com.cba.omnia.maestro.core.data.Field[au.com.cba.omnia.maestro.macros.thrift.Account,Int] = Field(balance,<function1>)
 * }}}
 * ''The following evaluation shows all the fields available to use for the thrift type [[Account]]''
 * {{{
 * scala> val fields = a.Fields
 * fields: AnyRef {
 *           val Id: au.com.cba.omnia.maestro.core.data.Field[au.com.cba.omnia.maestro.macros.thrift.Account,String];
 *           val Customer: au.com.cba.omnia.maestro.core.data.Field[au.com.cba.omnia.maestro.macros.thrift.Account,String];
 *           val Balance: au.com.cba.omnia.maestro.core.data.Field[au.com.cba.omnia.maestro.macros.thrift.Account,Int];
 *           val BalanceCents: au.com.cba.omnia.maestro.core.data.Field[au.com.cba.omnia.maestro.macros.thrift.Account,Int];
 *           val EffectiveDate: au.com.cba.omnia.maestro.core.data.Field[au.com.cba.omnia.maestro.macros.thrift.Account,String];
 *           def AllFields: List[au.com.cba.omnia.maestro.core.data.Field[au.com.cba.omnia.maestro.macros.thrift.Account, _ >: Int with String]]
 *         } = $anon$1@1fbfa5f
 *
 * scala> fields.AllFields.contains(fields.Balance)
 * res3: Boolean = true
 * }}}
 * '''A second call to the Fields method will give a new instance of the FieldWrapper and so all its fields are new which
 * is illustrated below :-'''
 * {{{
 * scala> a.Fields
 * res5: AnyRef{...removed...} = $anon$1@3e21bbdc
 *
 * scala> fields == res5
 * res6: Boolean = false
 *
 * scala> fields.AllFields.contains(res5.Balance)
 * res7: Boolean = false
 *
 * scala> fields.AllFields.contains(fields.Balance)
 * res8: Boolean = true
 * }}}
 * @tparam A
 */
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
