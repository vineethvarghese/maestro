
package au.com.cba.omnia.maestro.schema
package pretty
import scala.util.parsing.json.{JSON}
import org.apache.commons.lang.StringEscapeUtils

/** Abstract Json documents */
sealed trait JsonDoc
{
  def fromNum:    Option[Double] =
    Some(this) .collect { case JsonNum(n)     => n }

  def fromString: Option[String] =
    Some(this) .collect { case JsonString(s)  => s }

  def fromList:   Option[Seq[JsonDoc]] =
    Some(this) .collect { case JsonList(l, _) => l }

  def fromMap:    Option[Seq[(String, JsonDoc)]] =
    Some(this) .collect { case JsonMap(m, _)  => m } 
}


/** An uninterpreted string. When this is pretty printed special characters
 *  will be escaped properly */
case class JsonString(
  str:    String)                   
  extends JsonDoc


/** A numeric value. 
 *  JSON does not distinguish between integral and floating point. */
case class JsonNum(
  num:    Double)
  extends JsonDoc


/** A list of Json things. */
case class JsonList(
  list:   Seq[JsonDoc],
  spread: Boolean = false)
  extends JsonDoc


/** A map of key-value pairs. 
 *  These are stored in a Scala Seq rather than a Map so that the textual ordering
 *  in the source file is preserved (for pretty printing). */
case class JsonMap(
  list:   Seq[(String, JsonDoc)],
  spread: Boolean = false)  
  extends JsonDoc


object JsonDoc {


  /** Render a Json doc as a string. */
  def render(indent: Int, json: JsonDoc): String =
    linesJsonDoc(indent, json)
      .mkString("\n")

  /** Convert a string back to a JsonDoc. 
   *  This uses the default Scala JSON parser, but we load it into our JsonDoc
   *  structure to make the parser's dynamically typed output easier to consume. 
   */
  def parse(str: String): Option[JsonDoc] = {

    /** ZipWith-style helper. */
    def map2[A,B,C](a: Option[A], b: Option[B])(f: (A,B) => C): Option[C] =
      a.flatMap { x => b.map { y => f(x, y) } }

    /** Missing Haskell-esque sequence function. */
    def sequence[A](a: List[Option[A]]): Option[List[A]] =
      a.foldRight[Option[List[A]]](Some(Nil))((x,y) => map2(x,y)(_ :: _)) 

    /** Load a parsed JSON double. */
    def eatDouble(tree: Any): Option[JsonDoc] =
      Some(tree) 
        .collect { case d: Double  => d }
        .map     (JsonNum(_))

    /** Load a parsed JSON string. */
    def eatString(tree: Any): Option[JsonDoc] =
      Some(tree)
        .collect { case s: String  => s }
        .map     (JsonString(_))

    /** Load a parsed JSON list. */
    def eatList  (tree: Any): Option[JsonDoc] =
      Some(tree)
        .collect { case l: List[_] => 
          sequence(l.map(eat(_))) }
        .flatten
        .map     (JsonList(_))

    /** Load a parsed JSON map. */
    def eatMap   (tree: Any): Option[JsonDoc] =
      Some(tree)
        .collect { case m: Map[_, _] =>
          sequence(m .toList .map { case (k, v) =>
            for { j <- eat(v) } yield (k.asInstanceOf[String], j) })}
        .flatten
        .map     (JsonMap(_, false))

    /** Load a parsed JSON thing. */
    def eat(tree: Any): Option[JsonDoc] = 
      List(eatDouble(tree), eatString(tree), eatList(tree), eatMap(tree))
        .flatten .headOption

    JSON.parseFull(str)
      .map (t => eat(t)) .flatten
  }


  /** Render a Json doc as a list of strings, with one string per line. */
  def linesJsonDoc(indent: Int, json: JsonDoc): Seq[String] = 
    json match { 
      case JsonString(s) =>
        Seq(showString(s))

      // Json does not distinguish between integral and floating point values,
      // but we pretty print whole numbers with no fractional part for niceness.
      case JsonNum(i) => 
        if (i % 1 == 0) Seq(i.longValue.toString)
        else            Seq(i.toString)

      case JsonList(list, spread) =>
        Seq("[" + list.map(j => render(indent, j)).mkString(",\n") + "]")

      case JsonMap (list, spread) => 
        // Spreading the map across multiple lines.
        if (spread) {

          val keyWidth: Int = 
            list.map { case (k, _) => k.length }. max

          val body: Seq[String] =
            list 
              .zipWithIndex
              .map { case ((k, v), i) 
                => (showString(k) 
                      + (" " * (keyWidth - k.length))
                      + ": " 
                      + render(indent + 2, v)
                      + (if (i != list.size - 1) ", " else "")) }

          List("{ ") ++
          (injectOnLast(" }", body)
            .map { s => " " * indent + s })
        }

        // Packing the map onto a single line.
        else {
          val body: Seq[String] =
            list 
              .zipWithIndex
              .map { case ((k, v), i)
                => (showString(k) + ": " 
                      + render(indent + 2, v)
                      + (if (i != list.size - 1) ", " else "")) }

          List("{ " +
           (injectOnLast(" }", body))
              .mkString)
        }
  }


  /** Add the first string to the end of the last string in the
   *  provided list. */
  def injectOnLast(str: String, strs: Seq[String]): Seq[String] = 
    if (strs.size == 0) strs
    else {
      val is  = strs.init
      val l   = strs.last

      is ++ List(l + str)
    }

  /** Show a string with outer double quotes, and escaped chars. */
  def showString(raw: String): String =
    "\"" + escape(raw) + "\""


  /** Escape special characters in a string */
  def escape(raw: String): String =
    StringEscapeUtils.escapeJava(raw)

}
