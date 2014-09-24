
package au.com.cba.omnia.maestro.schema
package pretty
import scala.util.parsing.json.{JSON}

/** Abstract Json documents */
sealed trait JsonDoc


/** An uninterpreted string. When this is pretty printed special characters
 *  will be escaped properly */
case class JsonString(
  str:    String)                   
  extends JsonDoc


/** A numeric value. JSON does not distinguish between integral and floating point */
case class JsonNum(
  num:    Double)
  extends JsonDoc


/** A map of key-value pairs. */
case class JsonMap(
  list:   Seq[(String, JsonDoc)],
  spread: Boolean = false)  
  extends JsonDoc


object JsonDoc {

  /** Render a Json doc as a string. */
  def render(indent: Int, json: JsonDoc): String =
    linesJsonDoc(indent, json)
      .mkString("\n")

  /** Convert a string back to a Json doc. */
  def parse(str: String): JsonDoc = {

    def eat(tree: Any): List[JsonDoc] = 
      List(Some(tree) .collect { case d: Double    => JsonNum(d) },
           Some(tree) .collect { case s: String    => JsonString(s) },
           Some(tree) .collect { case m: Map[_, _] => 
              JsonMap(
                  m .toSeq
                    .map { case (k, v) => (
                        k.asInstanceOf[String],
                        eat(v).head)}, false) })
        .flatten

    eat(JSON.parseFull(str)).head
  }

  /** Render a Json doc as a list of strings, with one string per line. */
  def linesJsonDoc(indent: Int, json: JsonDoc): Seq[String] = 
    json match { 
      case JsonString(s) =>
        Seq(escape(s))

      case JsonNum(i) =>
        Seq(i.toString)

      case JsonMap (list, spread) => 
        // Spreading the map across multiple lines.
        if (spread) {

          val keyWidth: Int = 
            list.map { case (k, _) => k.length }. max

          val body: Seq[String] =
            list 
              .zipWithIndex
              .map { case ((k, v), i) 
                => (escape(k) 
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
                => (escape(k) + ": " 
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


  /** Escape special characters in a string */
  def escape(raw: String): String = {
    import scala.reflect.runtime.universe._
    Literal(Constant(raw)).toString
  }

}
