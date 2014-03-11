package au.com.cba.omnia.maestro.macros

import au.com.cba.omnia.maestro.core.codec._
import com.twitter.scrooge._

import scala.reflect.macros.Context

object TagMacro {
  def impl[A <: ThriftStruct: c.WeakTypeTag](c: Context): c.Expr[Tag[A]] = {
    import c.universe._
    val companion = c.universe.weakTypeOf[A].typeSymbol.companionSymbol
    val entries = Inspect.fields[A](c)
    val fields = entries.map({
      case (method, field) =>
        val name = Literal(Constant(method.name.toString))
        val typ = c.universe.weakTypeOf[A]
        val extract = Function(List(ValDef(Modifiers(Flag.PARAM), newTermName("x"), TypeTree(), EmptyTree)), Select(Ident(newTermName("x")), method.name))
        q"""au.com.cba.omnia.maestro.core.data.Field[${typ}, ${method.returnType}]($name, ${extract})"""
    })
    c.Expr[Tag[A]](q"Tag(row => row.zip(List(..$fields)))")
  }
}
