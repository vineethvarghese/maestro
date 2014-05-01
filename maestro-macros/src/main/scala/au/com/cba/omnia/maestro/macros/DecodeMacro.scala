package au.com.cba.omnia.maestro.macros

import au.com.cba.omnia.maestro.core.codec._
import com.twitter.scrooge._

import scala.reflect.macros.Context

object DecodeMacro {
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
          try {
            val result = ${build(decodeVals(xs))}
            DecodeOk((ValDecodeSource(vs.drop($size)), position + $size, result))
          } catch {
            case DecodeException(reason, index) => DecodeError(ValDecodeSource(vs.drop(index - position)), position + index, reason)
          }
        }
      }
    """

    def decodeUnknownSource(xs: List[(MethodSymbol, Int)]) = q"""
      def decodeUnknowns(vs: List[String], position: Int): DecodeResult[(DecodeSource, Int, $typ)] = {
        if (vs.length < $size) {
          DecodeError(UnknownDecodeSource(vs), position, NotEnoughInput($size, $typeName))
        } else {
          val fields = vs.take($size).toArray
          try {
            val result = ${build(decodeUnknowns(xs))}
            DecodeOk((UnknownDecodeSource(vs.drop($size)), position + $size, result))
          } catch {
            case DecodeException(reason, index) => DecodeError(UnknownDecodeSource(vs.drop(index - position)), position + index, reason)
          }
        }
      }
    """

    def build(args: List[c.universe.Apply]) = Apply(Select(Ident(companion), newTermName("apply")), args)

    def decodeVals(xs: List[(MethodSymbol, Int)]): List[c.universe.Apply] = xs.map { 
      case (x, i) => {
        val name = newTermName("as" + x.returnType + "Val")
        q"""DecodeUtil.$name(fields(${i - 1}), ${i - 1})"""
      }
    }

    def decodeUnknowns(xs: List[(MethodSymbol, Int)]): List[c.universe.Apply] = xs.map {
      case (x, i) if x.returnType == stringType => q"fields(${i - 1})"
      case (x, i)                               => {
        val name = newTermName("as" + x.returnType + "Unknown")
        q"""DecodeUtil.$name(fields(${i - 1}), ${i - 1})"""
      }
    }

    val combined = q"""Decode((source, position) => {
      import au.com.cba.omnia.maestro.core.data.Val
      import au.com.cba.omnia.maestro.core.codec.{DecodeSource, ValDecodeSource, UnknownDecodeSource, DecodeOk, DecodeError, DecodeResult, ParseError, ValTypeMismatch, NotEnoughInput, DecodeUtil}

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
