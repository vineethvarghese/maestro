package au.com.cba.omnia.maestro.macros

import scala.reflect.macros.Context

import com.twitter.scrooge._

import au.com.cba.omnia.maestro.core.codec._

import au.com.cba.omnia.humbug.HumbugThriftStruct

object Inspect {
  val ProductField = """_(\d+)""".r

  def indexed[A <: ThriftStruct: c.WeakTypeTag](c: Context): List[(c.universe.MethodSymbol, Int)] = 
    indexedUnsafe(c)(c.universe.weakTypeOf[A])

  def indexedUnsafe(c: Context)(typ: c.universe.Type): List[(c.universe.MethodSymbol, Int)] = {
    import c.universe._
    typ.members.toList.map(member => (member, member.name.toString)).collect({
      case (member, ProductField(n)) =>
        (member.asMethod, n.toInt)
    }).sortBy(_._2)
  }

  def fields[A <: ThriftStruct: c.WeakTypeTag](c: Context): List[(c.universe.MethodSymbol, String)] =
    fieldsUnsafe(c)(c.universe.weakTypeOf[A])


  def fieldsUnsafe(c: Context)(typ: c.universe.Type): List[(c.universe.MethodSymbol, String)] = {
    import c.universe._

    val fields =
      if (typ <:< c.universe.weakTypeOf[HumbugThriftStruct])
        typ.declarations.toList.collect {
          case sym: TermSymbol if sym.isVar => sym.name.toString.trim.capitalize
        }
      else
        typ.typeSymbol.companionSymbol.typeSignature
          .member(newTermName("apply")).asMethod.paramss.head.map(_.name.toString.capitalize)

    methodsUnsafe(c)(typ).zip(fields)
  }

  def methods[A <: ThriftStruct: c.WeakTypeTag](c: Context): List[c.universe.MethodSymbol] =
    indexed(c).map({ case (method, _) => method })


  def methodsUnsafe(c: Context)(typ: c.universe.Type): List[c.universe.MethodSymbol] =
    indexedUnsafe(c)(typ).map({ case (method, _) => method })
}
