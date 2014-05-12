package au.com.cba.omnia.maestro.macros

import scala.reflect.macros.Context

import com.twitter.scrooge._

import au.com.cba.omnia.maestro.core.codec._

import au.com.cba.omnia.humbug.HumbugThriftStruct

object Inspect {
  val ProductField = """_(\d+)""".r

  def indexed[A <: ThriftStruct: c.WeakTypeTag](c: Context): List[(c.universe.MethodSymbol, Int)] = {
    import c.universe._
    c.universe.weakTypeOf[A].members.toList.map(member => (member, member.name.toString)).collect({
      case (member, ProductField(n)) =>
        (member.asMethod, n.toInt)
    }).sortBy(_._2)
  }

  def fields[A <: ThriftStruct: c.WeakTypeTag](c: Context): List[(c.universe.MethodSymbol, String)] = {
    import c.universe._

    val fields =
      if (c.universe.weakTypeOf[A] <:< c.universe.weakTypeOf[HumbugThriftStruct])
        c.universe.weakTypeOf[A].members.toList
          .map(_.name.toString)
          .filter(x => x.endsWith("_$eq") && !x.startsWith("_"))
          .map(_.replaceAll("_\\$eq$", "").capitalize)
          .reverse
      else
        c.universe.weakTypeOf[A].typeSymbol.companionSymbol.typeSignature.members.toList
          .map(_.name.toString)
          .filter(x =>
            x.endsWith("Field") && !x.startsWith("write")
          ).map(_.replaceAll("Field$", ""))
          .reverse

    methods(c).zip(fields)
  }

  def methods[A <: ThriftStruct: c.WeakTypeTag](c: Context): List[c.universe.MethodSymbol] =
    indexed(c).map({ case (method, _) => method })
}
