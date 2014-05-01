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
          var index = 0
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

    def decodeVals(xs: List[(MethodSymbol, Int)]): List[Match] = xs.map { 
      case (x, i) => {
        val typeVal = newTermName(x.returnType + "Val")
        val name    = x.returnType.toString
        q"""
          fields(${i - 1}) match {
            case $typeVal(s) => {
              index +=1
              s
            }
            case x        =>
              return DecodeError(ValDecodeSource(vs.drop(index - position)), position + index, ValTypeMismatch(x, $name))
          }
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
