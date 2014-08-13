package au.com.cba.omnia.maestro.test

import scala.collection.JavaConverters._

import scalaz.effect.IO

import com.google.common.base.Splitter

import au.com.cba.omnia.thermometer.context.Context
import au.com.cba.omnia.thermometer.core.ThermometerRecordReader

import au.com.cba.omnia.maestro.core.codec._

trait Records {
  /**
   * Thermometer record reader for reading records from delimited text.
   * If the record `A` is a `ThriftStruct`, you can get the decoder by `Macros.mkDecode[A]`
   **/
  def delimitedThermometerRecordReader[A](delimiter: Char, decoder: Decode[A]) =
    ThermometerRecordReader((conf, path) => IO {
      val splitter = Splitter.on(delimiter)

      new Context(conf).lines(path).map(l =>
        decoder.decode(UnknownDecodeSource(splitter.split(l).asScala.toList)) match {
          case DecodeOk(c)       => c
          case e: DecodeError[A] => throw new Exception(s"Can't decode line $l in $path: $e")
        })
    })
}
