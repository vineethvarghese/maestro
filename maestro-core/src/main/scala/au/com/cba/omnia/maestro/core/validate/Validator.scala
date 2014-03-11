package au.com.cba.omnia.maestro.core
package validate

import au.com.cba.omnia.maestro.core.data._
import scalaz._, Scalaz._

case class Validator[A](run: A => ValidationNel[String, A])

object Validator {
  def all[A](validators: Validator[A]*): Validator[A] =
    Validator(a => validators.toList.traverseU(_.run(a)).as(a))

  def by[A](validation: A => Boolean, message: String): Validator[A] =
    Validator(a => if (validation(a)) a.success else message.failNel)

  def of[A, B](field: Field[A, B], check: Validator[B]): Validator[A] =
    Validator(a => check.run(field.get(a)).as(a))
}

object Check {
  def oneOf(categories: String*): Validator[String] =
    Validator(s => if (categories.contains(s)) s.success else s"""Expected one of [${categories.mkString("|")}] but got ${s}.""".failNel)

  def nonempty: Validator[String] =
    Validator(s => if (!s.isEmpty) s.success else "Value can not be empty.".failNel)
}
