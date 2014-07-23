package au.com.cba.omnia.maestro.test

import com.twitter.scrooge.ThriftStruct

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import scalaz.effect.IO

import au.com.cba.omnia.ebenezer.scrooge.ParquetScroogeTools

import au.com.cba.omnia.maestro.core.codec._

import au.com.cba.omnia.thermometer.context.Context
import au.com.cba.omnia.thermometer.core.Thermometer._
import au.com.cba.omnia.thermometer.core.ThermometerRecordReader
import au.com.cba.omnia.thermometer.context.Context

trait Records {

  object ParquetThermometerRecordReader {
    def apply[A <: ThriftStruct: Manifest] =
      ThermometerRecordReader((conf, path) => IO {
        ParquetScroogeTools.listFromPath[A](conf, path)
      })
  }

  def delimitedThermometerRecordReader[A](delimiter: Char, decoder: Decode[A]) =
    ThermometerRecordReader((conf, path) => IO {
      new Context(conf).lines(path).map(l =>
        decoder.decode(UnknownDecodeSource(l.split(delimiter).toList)) match {
          case DecodeOk(c) => Some(c)
          case _           => None
        }).flatten
    })
}