
package au.com.cba.omnia.maestro.schema
package pretty

/** Abstract Json documents */
sealed trait JsonDoc


/** An uninterpreted string. When this is pretty printed special characters
 *  will be escaped properly */
case class JsonString(
  str:    String)                   
  extends JsonDoc


/** A map of key-value pairs. */
case class JsonMap(
  list:   List[(String, JsonDoc)],
  spread: Boolean = false)  
  extends JsonDoc


object JsonDoc {

  /** Render a Json doc as a string. */
  def render(indent: Int, json: JsonDoc): String =
    linesJsonDoc(indent, json)
      .mkString("\n")


  /** Render a Json doc as a list of strings, with one string per line. */
  def linesJsonDoc(indent: Int, json: JsonDoc): List[String] = 
    json match { 
      case JsonString(s) =>
        List(s)

      case JsonMap (list, spread) => 
        if (spread) {

          val body: List[String] =
            list 
              .zipWithIndex
              .map { case ((k, v), i) 
                => (escape(k) + ": " 
                      + render(indent + 2, v)
                      + (if (i != list.size - 1) ", " else "")) }

          List("{ ") ++
          (injectOnLast(" }", body)
            .map { s => " " * indent + s })
        }

        else {

          val body: List[String] =
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
  def injectOnLast(str: String, strs: List[String]): List[String] = 
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
