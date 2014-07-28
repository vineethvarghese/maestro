package au.com.cba.omnia.maestro.test

import com.twitter.scrooge.ThriftStruct

import scalaz.effect.IO

import au.com.cba.omnia.ebenezer.scrooge.ParquetScroogeTools

import au.com.cba.omnia.maestro.core.codec._

import au.com.cba.omnia.thermometer.context.Context
import au.com.cba.omnia.thermometer.core.Thermometer._
import au.com.cba.omnia.thermometer.core.ThermometerRecordReader

trait Records {

  /**
   * Thermometer record reader for reading `ThriftStruct` records from a parquet file.
   **/
  object ParquetThermometerRecordReader {
    def apply[A <: ThriftStruct: Manifest] =
      ThermometerRecordReader((conf, path) => IO {
        ParquetScroogeTools.listFromPath[A](conf, path)
      })
  }

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