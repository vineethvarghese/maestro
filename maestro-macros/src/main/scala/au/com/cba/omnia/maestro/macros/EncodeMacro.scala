package au.com.cba.omnia.maestro.macros

import au.com.cba.omnia.maestro.core.codec._
import com.twitter.scrooge._

import scala.reflect.macros.Context

object EncodeMacro {
  def impl[A <: ThriftStruct: c.WeakTypeTag](c: Context): c.Expr[Encode[A]] = {
    import c.universe._
    val companion = c.universe.weakTypeOf[A].typeSymbol.companionSymbol
    val members = Inspect.methods[A](c)

    def encode(xs: List[MethodSymbol]): List[Tree] = xs.map(x => {
      val encoder = newTermName(x.returnType + "Val")
      q"""$encoder.apply(a.${x})"""
    })

    val fields = q"""List(..${encode(members)})"""

    c.Expr[Encode[A]](q"""Encode(a => {
      import au.com.cba.omnia.maestro.core.data.{Val, BooleanVal, IntVal, LongVal, DoubleVal, StringVal}

      $fields
     })""")
  }
}
