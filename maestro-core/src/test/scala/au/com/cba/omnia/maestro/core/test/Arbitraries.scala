package au.com.cba.omnia.maestro.core
package test

import au.com.cba.omnia.maestro.core.data._
import au.com.cba.omnia.maestro.core.codec._

import scalaz._, Scalaz._, \&/._
import scalaz.scalacheck.ScalazArbitrary._
import scalaz.scalacheck.ScalaCheckBinding._
import org.scalacheck._, Arbitrary._

import org.apache.thrift.protocol.TField
import org.apache.thrift.protocol.TType


object Arbitraries {
  implicit def ReasonArbitrary: Arbitrary[Reason] =
    Arbitrary(Gen.oneOf(
      (arbitrary[Val] |@| arbitrary[String])(ValTypeMismatch.apply)
    , (arbitrary[String] |@| arbitrary[String] |@| arbitrary[These[String, Throwable]])(ParseError.apply)
    , (arbitrary[Int] |@| arbitrary[String])(NotEnoughInput)
    , TooMuchInput
    ))

  implicit def DecodeResultArbitrary[A: Arbitrary]: Arbitrary[DecodeResult[A]] =
    Arbitrary(Gen.frequency(
      (4, arbitrary[A] map DecodeResult.ok)
    , (1, (arbitrary[List[Val]] |@| arbitrary[Int] |@| arbitrary[Reason])((vs, n, reason) => DecodeError(ValDecodeSource(vs), n, reason)))
    , (1, (arbitrary[List[String]] |@| arbitrary[Int] |@| arbitrary[Reason])((ss, n, reason) => DecodeError(UnknownDecodeSource(ss), n, reason)))
    ))

  implicit def ValArbitrary: Arbitrary[Val] =
    Arbitrary(Gen.oneOf(
        arbitrary[Boolean] map BooleanVal
      , arbitrary[Int] map IntVal
      , arbitrary[Long] map LongVal
      , arbitrary[Double] map DoubleVal
      , arbitrary[String] map StringVal
      ))

  implicit def TTypeGen: Gen[Byte] =
    Gen.oneOf(
      TType.BOOL,
      TType.BYTE,
      TType.DOUBLE,
      TType.ENUM,
      TType.I16,
      TType.I32,
      TType.I64,
      TType.LIST,
      TType.MAP,
      TType.SET,
      TType.STOP,
      TType.STRING,
      TType.STRUCT,
      TType.VOID)

  implicit def TFieldArbitrary: Arbitrary[TField] =
    Arbitrary((arbitrary[String] |@| arbitrary[Byte] |@| arbitrary[Short])((s, b, c) => new TField(s, b, c)))

  implicit def EncodeIntArbitrary: Arbitrary[Encode[Int]] =
    Arbitrary(Gen.oneOf(Encode.IntEncode :: Nil))

  implicit def DecodeIntArbitrary: Arbitrary[Decode[Int]] =
    Arbitrary(Gen.oneOf(Decode.IntDecode :: Nil))

  implicit def DecodeIntIntArbitrary: Arbitrary[Decode[Int => Int]] =
    Arbitrary(arbitrary[Int => Int] map (_.point[Decode]))
}
