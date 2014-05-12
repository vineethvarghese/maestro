package au.com.cba.omnia.maestro.macros

import scala.reflect.macros.Context

import com.twitter.scrooge._

import au.com.cba.omnia.humbug.HumbugThriftStruct

import au.com.cba.omnia.maestro.core.codec._

object DecodeMacro {
  def impl[A <: ThriftStruct: c.WeakTypeTag](c: Context): c.Expr[Decode[A]] = {
    val typ       = c.universe.weakTypeOf[A]
    val humbugTyp = c.universe.weakTypeOf[HumbugThriftStruct]

    typ match {
      case t if t <:< humbugTyp => HumbugDecodeMacro.impl(c)
      case _                    =>  ScroogeDecodeMacro.impl(c)
    }
  }
}
