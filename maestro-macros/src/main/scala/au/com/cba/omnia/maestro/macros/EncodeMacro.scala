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

import scala.reflect.macros.Context

import com.twitter.scrooge.ThriftStruct

import au.com.cba.omnia.maestro.core.codec.Encode

/**
  * Converts a thrift struct into a list of its fields.
  * 
  * Each field is encoded as the [[Val]] corresponding to its type.
  */
object EncodeMacro {
  def impl[A <: ThriftStruct: c.WeakTypeTag](c: Context): c.Expr[Encode[A]] = {
    import c.universe._

    val companion = c.universe.weakTypeOf[A].typeSymbol.companionSymbol
    val members   = Inspect.methods[A](c)

    def encode(xs: List[MethodSymbol]): List[Tree] = xs.map { x =>
      MacroUtils.optional(c)(x.returnType).map { param =>
        val encoder = newTermName(param  + "Val")
        q"a.${x}.map($encoder.apply).getOrElse(NoneVal)"
      } getOrElse {
        val encoder = newTermName(x.returnType + "Val")
        q"""$encoder.apply(a.${x})"""
      }
    }

    val fields = q"""List(..${encode(members)})"""

    c.Expr[Encode[A]](q"""Encode(a => {
      import au.com.cba.omnia.maestro.core.data.{Val, BooleanVal, IntVal, LongVal, DoubleVal, StringVal, NoneVal}

      $fields
     })""")
  }
}
