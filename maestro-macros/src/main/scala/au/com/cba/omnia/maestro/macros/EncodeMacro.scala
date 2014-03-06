package au.com.cba.omnia.maestro.macros

import au.com.cba.omnia.maestro.core.codec._
import com.twitter.scrooge._

import scala.reflect.macros.Context

object EncodeMacro {
  def impl[A <: ThriftStruct: c.WeakTypeTag](c: Context): c.Expr[Encode[A]] = {
    import c.universe._
    val companion = c.universe.weakTypeOf[A].typeSymbol.companionSymbol
    val members = Inspect.methods[A](c)
    val fields = members.map(m => q"""Encode.encode[${m.returnType}](a.${m})""").reduceLeft((acc, x) => q"$acc ++ $x")
    c.Expr[Encode[A]](q"Encode(a => $fields)")
  }
}
