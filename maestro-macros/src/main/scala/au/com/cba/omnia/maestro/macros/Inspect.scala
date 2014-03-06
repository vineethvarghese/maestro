package au.com.cba.omnia.maestro.macros

import au.com.cba.omnia.maestro.core.codec._
import com.twitter.scrooge._

import scala.reflect.macros.Context

object Inspect {
  val ProductField = """_(\d+)""".r

  def indexed[A <: ThriftStruct: c.WeakTypeTag](c: Context): List[(c.universe.MethodSymbol, Int)] = {
    import c.universe._
    c.universe.weakTypeOf[A].members.toList.map(member => (member, member.name.toString)).collect({
      case (member, ProductField(n)) =>
        (member.asMethod, n.toInt)
    }).sortBy(_._2)
  }

  def fields[A <: ThriftStruct: c.WeakTypeTag](c: Context): List[(c.universe.MethodSymbol, String)] =
    methods(c).zip(c.universe.weakTypeOf[A].typeSymbol.companionSymbol.typeSignature.members.toList.filter(x =>
      x.name.toString.endsWith("Field") && !x.name.toString.startsWith("write")
    ).map(v => v.name.toString.replaceAll("Field$", "")).reverse)


  def methods[A <: ThriftStruct: c.WeakTypeTag](c: Context): List[c.universe.MethodSymbol] =
    indexed(c).map({ case (method, _) => method })
}
