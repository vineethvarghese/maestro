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

import com.twitter.scrooge._

import au.com.cba.omnia.maestro.core.codec._
import au.com.cba.omnia.maestro.core.data._

import au.com.cba.omnia.humbug.HumbugThriftStruct

object SplitMacro {
  def impl[A <: ThriftStruct : c.WeakTypeTag, B <: Product  : c.WeakTypeTag](c: Context): c.Expr[A => B] = {
    import c.universe._

    val srcType = c.universe.weakTypeOf[A]
    val productType = c.universe.weakTypeOf[B]
    val thriftStructType = weakTypeOf[ThriftStruct]
    val humbugType = weakTypeOf[HumbugThriftStruct]

    val in = newTermName(c.fresh)

    // Get the split target types in declaration order.
    val targetTypes = productType.declarations.sorted.toList collect {
      case sym: TermSymbol if sym.isVal && sym.isCaseAccessor => sym.typeSignatureIn(productType)
    }

    def abort(msg: String) =
      c.abort(c.enclosingPosition, s"Can't create split for $srcType to $productType: $msg")

    def createSubset(typ: Type): Tree = {
      val fields = Inspect.fieldsUnsafe(c)(typ).map(f => newTermName(decapitalize(f._2)))

      if (typ <:< c.universe.weakTypeOf[HumbugThriftStruct]) {
        val tmp = newTermName(c.fresh)
        val assignments = fields.map(f => q"$tmp.$f = $in.$f")

        q"""{
          val $tmp = new $typ()
          ..$assignments
          $tmp
        }
        """
      } else {
        val assignments = fields.map(f => q"$in.$f")
        val companion = typ.typeSymbol.companionSymbol
        q"""$companion.apply(..$assignments)"""
      }
    }

    if (targetTypes.exists(t => !(t <:<  thriftStructType))) {
      val invalidTypes = targetTypes.filter(t => !(t <:<  thriftStructType))
      abort(s"""${invalidTypes.mkString(", ")} are not ThriftStructs""")
    }
    
    val sourceFields = Inspect.fields[A](c).map { case (m, n) => (m.returnType, decapitalize(n)) }.toSet
    val targetFields =
      targetTypes.flatMap(Inspect.fieldsUnsafe(c)(_)).map { case (m, n) => (m.returnType, decapitalize(n)) }.toSet

    if (!targetFields.subsetOf(sourceFields)) {
      val missingFields = targetFields.filter(f => !sourceFields.contains(f)).map { case (t, n) => s"$n: $t" }
      abort(s"""$srcType does not have ${missingFields.mkString(", ")}""")
    }

    val tuple = newTermName(s"Tuple${targetTypes.length}")
    val constructors: List[Tree] = targetTypes.map(t => createSubset(t))

    val x: ValDef = q"val $in: $srcType"
    val result = q"($in: $srcType) => $tuple.apply(..$constructors)"

    c.Expr[Function1[A, B]](result)

  }

  def decapitalize(s: String): String =
    if (s.length == 0) s
    else Character.toLowerCase(s.head) +: s.tail
}
