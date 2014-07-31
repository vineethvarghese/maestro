package au.com.cba.omnia.maestro.test

import scalaz.effect.IO

import au.com.cba.omnia.maestro.core.codec._

import au.com.cba.omnia.thermometer.context.Context
import au.com.cba.omnia.thermometer.core.ThermometerRecordReader

trait Records {
  /**
   * Thermometer record reader for reading records from delimited text.
   * If the record `A` is a `ThriftStruct`, you can get the decoder by `Macros.mkDecode[A]`
   **/
  def delimitedThermometerRecordReader[A](delimiter: Char, decoder: Decode[A]) =
    ThermometerRecordReader((conf, path) => IO {
      new Context(conf).lines(path).map(l =>
        decoder.decode(UnknownDecodeSource(l.split(delimiter).toList)) match {
          case DecodeOk(c) => Some(c)
          case _           => None
        }).flatten
    })
}
