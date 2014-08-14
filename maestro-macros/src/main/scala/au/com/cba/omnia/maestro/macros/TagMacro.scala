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

import au.com.cba.omnia.maestro.core.codec.Tag

object TagMacro {
  def impl[A <: ThriftStruct: c.WeakTypeTag](c: Context): c.Expr[Tag[A]] = {
    import c.universe._

    val companion = c.universe.weakTypeOf[A].typeSymbol.companionSymbol
    val entries   = Inspect.fields[A](c)

    val fields = entries.map({
      case (method, field) =>
        val name    = Literal(Constant(method.name.toString))
        val typ     = c.universe.weakTypeOf[A]
        val extract = Function(
          List(ValDef(Modifiers(Flag.PARAM), newTermName("x"), TypeTree(), EmptyTree)),
          Select(Ident(newTermName("x")), method.name)
        )

        q"au.com.cba.omnia.maestro.core.data.Field[${typ}, ${method.returnType}]($name, ${extract})"
    })

    val result = q"""
      import au.com.cba.omnia.maestro.core.codec.Tag
      Tag(row => row.zip(List(..$fields)))
    """
    c.Expr[Tag[A]](result)
  }
}
