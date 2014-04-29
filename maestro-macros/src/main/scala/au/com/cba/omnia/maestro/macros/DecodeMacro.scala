package au.com.cba.omnia.maestro.macros

import au.com.cba.omnia.maestro.core.codec._
import com.twitter.scrooge._

import scala.reflect.macros.Context

object DecodeMacro {
  val ChunkSize = 15


  /*
   * This builds up a chunked Decode instances, chunking flatMap codes by 22
   * so that there is not code size blow-out with > 100 fields (it can quickly
   * reach JVM method size limits).
   *
   * The easiest way to visualize this code is, for some apply method:
   *
   * {{{
   * object Person {
   *   def apply(name: String, age: Int, address: String) = ...
   * }
   * }}}
   *
   * We end up with (the de-sugared equivalent of):
   *
   * {{{
   * for {
   *   x_1 <- implicitly[Decode[(String, Int, String)]]
   * } yield Person.apply(x_1._1, x_1._2, x_1._3)
   * }}}
   *
   * Extended to > 22 we end up with something like:
   *
   * {{{
   * for {
   *   x_1 <- implicitly[Decode[(String, Int, String......)]]
   *   x_2 <- implicitly[Decode[(String, Int, String......)]]
   * } yield Big.apply(x_1._1, x_1._2, x_1._3, ....., x_1._22, x_2._1, x_2._2 ....)
   * }}}
   *
   *
   * This is still not the optimal way to do things, in that we could revolve the implicits
   * at macro expansion, and potentially inline more calls by hand, but the current implementation
   * is good enough for now, in that it at leasrt scales linearly and doesn't break the jvm.
   */
  def impl[A <: ThriftStruct: c.WeakTypeTag](c: Context): c.Expr[Decode[A]] = {
    import c.universe._
    val companion = c.universe.weakTypeOf[A].typeSymbol.companionSymbol
    val (members, indexes) = Inspect.indexed[A](c).unzip

    def codecTupleOf(xs: List[MethodSymbol]): Tree = xs match {
      case a :: Nil =>
        q"implicitly[Decode[${a.returnType}]]"
      case as if as.length <= ChunkSize =>
        TypeApply(Ident(newTermName("implicitly")),
          List(AppliedTypeTree(Ident(newTypeName("Decode")),
            List(AppliedTypeTree(Ident(newTypeName(s"Tuple${as.length}")),
              as.map(a => q"${a.returnType}"))))))
      case _  =>
        sys.error("can't handle something with more than 15 elements")
    }

    @annotation.tailrec
    def bind(base: Tree, xs: List[(List[MethodSymbol], Int)], method: String): Tree = xs match {
        case Nil =>
          base
        case (chunk, idx) :: tail =>
          bind(Apply(Select(codecTupleOf(chunk), newTermName(method)), List(
                 Function(List(ValDef(Modifiers(Flag.PARAM), newTermName("x_" + idx), TypeTree(), EmptyTree)), base))), tail, "flatMap")
    }

    val args = indexes.map(v => {
      val chunk = (v.toInt - 1) / ChunkSize
      val fragment = ((v.toInt - 1) % ChunkSize) + 1
      Select(Ident(newTermName(s"x_$chunk")), newTermName(s"_$fragment"))
    })
    val yield_ = Apply(Select(Ident(companion), newTermName("apply")), args)
    val result = bind(yield_, members.grouped(ChunkSize).toList.zipWithIndex.reverse, "map")
    c.Expr[Decode[A]](result)
  }
}
