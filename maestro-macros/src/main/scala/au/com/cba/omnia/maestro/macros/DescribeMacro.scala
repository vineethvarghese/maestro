package au.com.cba.omnia.maestro.macros

import au.com.cba.omnia.maestro.core.codec._
import com.twitter.scrooge._

import scala.reflect.macros.{Universe, Context}

object DescribeMacro {
  def impl[A <: ThriftStruct: c.WeakTypeTag](c: Context): c.Expr[Describe[A]] = {
    import c.universe._
    val typ = c.universe.weakTypeOf[A].typeSymbol
    val companion = typ.companionSymbol
    val entries = Inspect.fields[A](c)
    val fields = entries.map({
      case (method, field) =>
        val name = Literal(Constant(field))
        val tfield =  Select(Ident(companion), newTermName(field + "Field"))
        q"""($name, ${tfield})"""
    })
 //   println(showRaw(q"")))
 //   println(show(Apply(Select(Ident(../)))))
    val tablename = Literal(Constant(typ.name.toString))
    c.Expr[Describe[A]](q"au.com.cba.omnia.maestro.core.codec.Describe($tablename, List(..$fields))")
  }
}
