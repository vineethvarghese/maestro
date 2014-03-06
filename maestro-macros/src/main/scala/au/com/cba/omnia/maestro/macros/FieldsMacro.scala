package au.com.cba.omnia.maestro.macros

import au.com.cba.omnia.maestro.core.codec._
import au.com.cba.omnia.maestro.core.data._
import com.twitter.scrooge._

import scala.reflect.macros.Context
import scala.annotation.StaticAnnotation

class body(tree: Any) extends StaticAnnotation

object FieldsMacro {
  def impl[A <: ThriftStruct: c.WeakTypeTag](c: Context) = {
    import c.universe._

    val entries = Inspect.fields[A](c)
    val fields = entries.map({
      case (method, field) =>
        val name = Literal(Constant(method.name.toString))
        val typ = c.universe.weakTypeOf[A]
        val extract = Function(List(ValDef(Modifiers(Flag.PARAM), newTermName("x"), TypeTree(), EmptyTree)), Select(Ident(newTermName("x")), method.name))
        (method, field, q"""au.com.cba.omnia.maestro.core.data.Field[${typ}, ${method.returnType}]($name, ${extract})""")
    }).map({
      case (method, field, value) =>
        val n = newTermName(field)
        q"""def ${n} = $value"""
    })
    val r =q"class FieldsWrapper { ..$fields }; new FieldsWrapper {}"
    c.Expr(r)
  }
}
