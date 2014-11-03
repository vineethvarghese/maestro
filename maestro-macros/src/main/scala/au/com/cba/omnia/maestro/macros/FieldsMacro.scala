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

import au.com.cba.omnia.maestro.core.codec._
import au.com.cba.omnia.maestro.core.data._
import com.twitter.scrooge._

import scala.reflect.macros.Context
import scala.annotation.StaticAnnotation

class body(tree: Any) extends StaticAnnotation

/**
  * A macro that generates a `Field` for every field in the thrift struct. All the fields are
  * members of FieldsWrapper.
  */
object FieldsMacro {
  def impl[A <: ThriftStruct: c.WeakTypeTag](c: Context) = {
    import c.universe._

    val typ       = c.universe.weakTypeOf[A]
    val entries   = Inspect.fields[A](c)
    val companion = typ.typeSymbol.companionSymbol
    val nameGetter = newTermName("name")

    val fields = entries.map({
      case (method, field) =>
        val name    = q"""$companion.${newTermName(field + "Field")}.$nameGetter"""
        val extract = Function(List(ValDef(Modifiers(Flag.PARAM), newTermName("x"), TypeTree(), EmptyTree)), Select(Ident(newTermName("x")), method.name))
        (method, field, q"""au.com.cba.omnia.maestro.core.data.Field[${typ}, ${method.returnType}]($name, ${extract})""")
    }).map({
      case (method, field, value) =>
        val n = newTermName(field)
        q"""val ${n} = $value"""
    })
    val refs = entries.map({
      case (method, field) =>
        val n = newTermName(field)
        q"$n"
    })
    val r =q"class FieldsWrapper { ..$fields; def AllFields = List(..$refs) }; new FieldsWrapper {}"
    c.Expr(r)
  }
}
