package au.com.cba.omnia.maestro.core
package codec


import au.com.cba.omnia.maestro.core.data._
import scalaz._, Scalaz._
import org.apache.thrift.protocol.TField

case class Describe[A](name: String, metadata: List[(String, TField)])

object Describe {
  def describe[A: Describe]: List[(String, TField)] =
    Describe.of[A].metadata

  def of[A: Describe]: Describe[A] =
    implicitly[Describe[A]]
}

