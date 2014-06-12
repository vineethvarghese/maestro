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
import com.twitter.scrooge._

import scala.reflect.macros.Context


/**
  * Creates a custom Decode for the given thrift struct.
  * 
  * The Decode will look like this:
  * {{{
  * Decode[Customer]((source, position) => {
  *   source match {
  *     case ValDecodeSource(vs)     => decodeVals(vs, position)
  *     case UnknownDecodeSource(vs) => decodeUnknowns(vs, position)
  *   }
  * 
  *   def decodeVals(vs: List[Val], position: Int): DecodeResult[(DecodeSource, Int, Customer)] = {
  *     if (vs.length < 5) {
  *       DecodeError(ValDecodeSource(vs), position, NotEnoughInput(5, "CUSTOMER"))
  *     } else {
  *       val fields = vs.take(5).toArray
  *       val result = Customer.apply(
  *         if (fields(0).isInstanceOf[StringVal])
  *           fields(0).asInstanceOf[StringVal].v
  *         else
  *           return DecodeError(ValDecodeSource(vs.drop(0 - position)), position + 0, ValTypeMismatch(fields(0), "String")),
  *         if (fields(1).isInstanceOf[StringVal])
  *           fields(1).asInstanceOf[StringVal].v
  *         else
  *           return DecodeError(ValDecodeSource(vs.drop(1 - position)), position + 1, ValTypeMismatch(fields(1), "String")),
  *         if (fields(2).isInstanceOf[StringVal])
  *           fields(2).asInstanceOf[StringVal].v
  *         else
  *           return DecodeError(ValDecodeSource(vs.drop(2 - position)), position + 2, ValTypeMismatch(fields(2), "String")),
  *         if (fields(3).isInstanceOf[StringVal])
  *           fields(3).asInstanceOf[StringVal].v
  *         else
  *           return DecodeError(ValDecodeSource(vs.drop(3 - position)), position + 3, ValTypeMismatch(fields(3), "String")),
  *         if (fields(4).isInstanceOf[StringVal])
  *           fields(4).asInstanceOf[StringVal].v
  *         else
  *           return DecodeError(ValDecodeSource(vs.drop(4 - position)), position + 4, ValTypeMismatch(fields(4), "String")),
  *         if (fields(5).isInstanceOf[IntVal])
  *           fields(5).asInstanceOf[IntVal].v
  *         else
  *           return DecodeError(ValDecodeSource(vs.drop(5 - position)), position + 5, ValTypeMismatch(fields(5), "Int")),
  *         if (fields(6).isInstanceOf[StringVal])
  *           fields(6).asInstanceOf[StringVal].v
  *         else
  *           return DecodeError(ValDecodeSource(vs.drop(6 - position)), position + 6, ValTypeMismatch(fields(6), "String")));
  *       DecodeOk(Tuple3(ValDecodeSource(vs.drop(7)), position + 7, result))
  *     }
  *   }
  * 
  *   def decodeUnknowns(vs: List[String], position: Int): DecodeResult[Tuple3[DecodeSource, Int, Customer]] =
  *     if (vs.length < 7) DecodeError(UnknownDecodeSource(vs), position, NotEnoughInput(7, "Customer"))
  *     else {
  *       val fields = vs.take(7).toArray
  *       var index = -1
  *       var value = ""
  *       var tag = ""
  *       try {
  *         val result = Customer.apply(
  *           fields(0),
  *           fields(1),
  *           fields(2),
  *           fields(3),
  *           fields(4),
  *           {
  *             tag = "Int"
  *             index = 5
  *             fields(index).toInt
  *           },
  *           fields(6))
  *         DecodeOk(Tuple3(UnknownDecodeSource(vs.drop(7)), position + 7, result))
  *       } catch {
  *         case NonFatal((e @ _)) => DecodeError(UnknownDecodeSource(vs.drop(index - position)), position + index, ParseError(fields(index), tag, That(e)))
  *       }
  *     }
  *   })
  *  }}}
  */
object ScroogeDecodeMacro {
  def impl[A <: ThriftStruct: c.WeakTypeTag](c: Context): c.Expr[Decode[A]] = {
    import c.universe._

    val typ = c.universe.weakTypeOf[A]
    val typeName: String = typ.toString
    val stringType = c.universe.weakTypeOf[String]
    val companion = typ.typeSymbol.companionSymbol
    val members = Inspect.indexed[A](c)
    val size = members.length

    def decodeValSource(xs: List[(MethodSymbol, Int)]) = q"""
      def decodeVals(vs: List[Val], position: Int): DecodeResult[(DecodeSource, Int, $typ)] = {
        if (vs.length < $size) {
          DecodeError(ValDecodeSource(vs), position, NotEnoughInput($size, $typeName))
        } else {
          val fields = vs.take($size).toArray
          val result = ${build(decodeVals(xs))}
          DecodeOk((ValDecodeSource(vs.drop($size)), position + $size, result))
        }
      }
    """

    def decodeUnknownSource(xs: List[(MethodSymbol, Int)]) = q"""
      def decodeUnknowns(vs: List[String], position: Int): DecodeResult[(DecodeSource, Int, $typ)] = {
        if (vs.length < $size) {
          DecodeError(UnknownDecodeSource(vs), position, NotEnoughInput($size, $typeName))
        } else {
          val fields = vs.take($size).toArray
          var index = -1
          var value = ""
          var tag   = "" 
          try {
            val result = ${build(decodeUnknowns(xs))}
            DecodeOk((UnknownDecodeSource(vs.drop($size)), position + $size, result))
          } catch {
            case NonFatal(e) => DecodeError(
              UnknownDecodeSource(vs.drop(index - position)),
              position + index,
              ParseError(fields(index), tag, That(e))
            )
          }
        }
      }
    """

    def build(args: List[Tree]) = Apply(Select(Ident(companion), newTermName("apply")), args)

    def decodeVals(xs: List[(MethodSymbol, Int)]): List[Tree] = xs.map { 
      case (x, i) => {
        val typeVal = newTypeName(x.returnType + "Val")
        val name    = x.returnType.toString
        val index   = i - 1
        q"""
          if (fields($index).isInstanceOf[$typeVal])
            fields($index).asInstanceOf[$typeVal].v
          else
            return DecodeError(ValDecodeSource(vs.drop($index - position)), position + $index, ValTypeMismatch(fields($index), $name))
        """
      }
    }

    def decodeUnknowns(xs: List[(MethodSymbol, Int)]): List[Tree] = xs.map {
      case (x, i) if x.returnType == stringType => q"fields(${i - 1})"
      case (x, i)                               => {
        val method = newTermName("to" + x.returnType)
        val name    = x.returnType.toString
        q"""{
          tag    = $name
          index  = ${i - 1}
          fields(index).$method
        }"""
      }
    }

    //

    val combined = q"""Decode((source, position) => {
      import scala.util.control.NonFatal
      import scalaz.\&/.That
      import au.com.cba.omnia.maestro.core.data.{Val, BooleanVal, IntVal, LongVal, DoubleVal, StringVal}
      import au.com.cba.omnia.maestro.core.codec.{DecodeSource, ValDecodeSource, UnknownDecodeSource, DecodeOk, DecodeError, DecodeResult, ParseError, ValTypeMismatch, NotEnoughInput}

      ${decodeValSource(members)}
      ${decodeUnknownSource(members)}

      source match {
        case ValDecodeSource(vs)     => decodeVals(vs, position)
        case UnknownDecodeSource(vs) => decodeUnknowns(vs, position)
      }
    })
    """

    c.Expr[Decode[A]](combined)
  }
}
