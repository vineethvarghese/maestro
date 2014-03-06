package com.cba.omnia.maestro.core
package task


import au.com.cba.omnia.ebenezer.scrooge._
import com.cba.omnia.maestro.core.scalding._
import com.cba.omnia.maestro.core.codec._
import com.google.common.base.Splitter
import com.twitter.scalding._, TDsl._
import com.twitter.scrooge._

import scala.collection.JavaConverters._
import scalaz._, Scalaz._


object Load {
  def create[A <: ThriftStruct : Decode : Manifest](args: Args, delimiter: String, sources: List[String], output: String, errors: String, now: String) =
    new LoadJob(args, delimiter, sources, output, errors, now)
}

class LoadJob[A <: ThriftStruct : Decode : Manifest](args: Args, delimiter: String, sources: List[String], output: String, errors: String, now: String) extends UniqueJob(args) {

  Errors.safely(errors) {

    sources.map(p => TextLine(p).read).reduceLeft(RichPipe(_) ++ RichPipe(_))
      .toTypedPipe[String]('line)
      .map(line => Splitter.on(delimiter).split(line).asScala.toList)
      .map(record => record.map(_.trim))
      .map(record => Decode.decode[A](UnknownDecodeSource(record ++ List(now))))
      .map({
        case DecodeOk(value) =>
          value.right
        // FIX this
        case e @ DecodeError(remainder, counter, reason) =>
          reason match {
            case ValTypeMismatch(value, expected) =>
              s"unexpected type: $e".left
            case ParseError(value, expected, error) =>
              s"unexpected type: $e".left
            case NotEnoughInput(required, expected) =>
              s"not enough fields in record: $e".left
            case TooMuchInput =>
              s"too many fields in record: $e".left
          }
       })

  }.write(ParquetScroogeSource[A](output))

}
