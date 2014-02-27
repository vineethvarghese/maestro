package com.cba.omnia.maestro.core
package codec

import com.cba.omnia.maestro.core.data._
import scala.util.control.NonFatal
import scalaz._, Scalaz._, \&/._
import shapeless.{ProductTypeClass, TypeClassCompanion}

sealed trait Reason
case class ValTypeMismatch(value: Val, expected: String) extends Reason
case class ParseError(value: String, expected: String, error: These[String, Throwable]) extends Reason
case class NotEnoughInput(required: Int, expected: String) extends Reason
case object TooMuchInput extends Reason

sealed trait DecodeResult[A] {
  def fold[X](ok: A => X, error: (DecodeSource, Int, Reason) => X): X = this match {
    case DecodeOk(value) => ok(value)
    case DecodeError(remainder, counter, reason) => error(remainder, counter, reason)
  }

  def map[B](f: A => B): DecodeResult[B] =
    flatMap(f andThen DecodeResult.ok)

  def flatMap[B](f: A => DecodeResult[B]): DecodeResult[B] =
    fold(f, DecodeResult.error)
}

case class DecodeOk[A](value: A) extends DecodeResult[A]
case class DecodeError[A](remainder: DecodeSource, counter: Int, reason: Reason) extends DecodeResult[A]

object DecodeResult {
  def ok[A](a: A): DecodeResult[A] =
    DecodeOk(a)

  def error[A](remainder: DecodeSource, counter: Int, reason: Reason): DecodeResult[A] =
    DecodeError(remainder, counter, reason)

  implicit def DecodeResultMonad: Monad[DecodeResult] = new Monad[DecodeResult] {
    def point[A](a: => A) = ok(a)
    def bind[A, B](m: DecodeResult[A])(f: A => DecodeResult[B]) = m flatMap f
  }

  implicit def DecodeResultEqual[A: Equal]: Equal[DecodeResult[A]] =
    Equal.equal({
      case (DecodeOk(a), DecodeOk(b)) => a === b
      case (a, b) => a == b
    })

}
