package au.com.cba.omnia.maestro.core
package codec

import au.com.cba.omnia.maestro.core.data._
import scalaz._, Scalaz._

case class Tag[A](run: List[String] => List[(String, Field[A, _])])

object Tag {
  def tag[A: Tag](row: List[String]): List[(String, Field[A, _])] =
    Tag.of[A].run(row)

  def of[A: Tag]: Tag[A] =
    implicitly[Tag[A]]
}
