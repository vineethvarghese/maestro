package au.com.cba.omnia.maestro.macros

import scala.reflect.macros.Context

import com.twitter.scrooge._

import au.com.cba.omnia.humbug.HumbugThriftStruct

import au.com.cba.omnia.maestro.core.codec._

object HumbugDecodeMacro {
  def impl[A <: ThriftStruct: c.WeakTypeTag](c: Context): c.Expr[Decode[A]] = {
    import c.universe._

    val typ = c.universe.weakTypeOf[A]
    val typeName: String = typ.toString
    val stringType = c.universe.weakTypeOf[String]
    val companion = typ.typeSymbol.companionSymbol
    val members = Inspect.indexed[A](c)
    val size = members.length
    val termName = newTermName(typeName)

    def decodeValSource(xs: List[(MethodSymbol, Int)]) = q"""
      def decodeVals(vs: List[Val], position: Int): DecodeResult[(DecodeSource, Int, $typ)] = {
        if (vs.length < $size) {
          DecodeError(ValDecodeSource(vs), position, NotEnoughInput($size, $typeName))
        } else {
          var index = -1
          var tag   = ""
          val fields = vs.take($size).toArray

          try {
          val struct = new $typ()
          ..${decodeVals(xs)}
          DecodeOk((ValDecodeSource(vs.drop($size)), position + $size, struct))
          } catch {
            case NonFatal(e) => DecodeError(
              ValDecodeSource(vs.drop(index - position)),
              position + index,
              ValTypeMismatch(fields(index), tag))
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
          var index = -1
          var tag   = "" 
          try {
            val struct = new $typ()
            ..${decodeUnknowns(xs)}
            DecodeOk((UnknownDecodeSource(vs.drop($size)), position + $size, struct))
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

    def decodeVals(xs: List[(MethodSymbol, Int)]): List[Tree] = xs.map { 
      case (x, i) => {
        val typeVal = newTypeName(x.returnType + "Val")
        val tag     = x.returnType.toString
        val name    = x.returnType.toString
        val index   = i - 1
        val setter = newTermName("_" + i)
        q"""
          index = $index
          tag   = $tag
          struct.$setter = fields($index).asInstanceOf[$typeVal].v
        """
      }
    }

    def decodeUnknowns(xs: List[(MethodSymbol, Int)]): List[Tree] = xs.map { case (x, i) =>
      val index  = i - 1
      val setter = newTermName("_" + i)
      if (x.returnType == stringType)
        q"struct.$setter = fields($index)"
      else {
        val method = newTermName("to" + x.returnType)
        val tag   = x.returnType.toString

        q"""{
          tag    = $tag
          index  = $index
          struct.$setter = fields(index).$method
        }"""
      }
    }

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
