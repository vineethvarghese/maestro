package au.com.cba.omnia.maestro.core
package task

import scala.collection.JavaConverters._
import scala.util.matching.Regex

import scalaz.{Tag => _, _}, Scalaz._

import com.google.common.base.Splitter

import com.twitter.scalding._, TDsl._

import com.twitter.scrooge.ThriftStruct

import au.com.cba.omnia.ebenezer.scrooge._

import au.com.cba.omnia.maestro.core.scalding._
import au.com.cba.omnia.maestro.core.codec._
import au.com.cba.omnia.maestro.core.clean._
import au.com.cba.omnia.maestro.core.validate._
import au.com.cba.omnia.maestro.core.filter._

sealed trait TimeSource {
  def getTime(path: String): String = this match {
    case Predetermined(time) => time
    case FromPath(extract)  => extract(path)
  }
}

case class Predetermined(time: String) extends TimeSource
case class FromPath(extract: String => String) extends TimeSource


object Load {
  def create[A <: ThriftStruct : Decode : Tag : Manifest](args: Args, delimiter: String, sources: List[String], output: String, errors: String, timeSource: TimeSource, clean: Clean, validator: Validator[A], filter: RowFilter) =
    new LoadJob(args, delimiter, sources, output, errors, timeSource, clean, validator, filter)
}

class LoadJob[A <: ThriftStruct : Decode: Tag : Manifest](args: Args, delimiter: String, sources: List[String], output: String, errors: String, timeSource: TimeSource, clean: Clean, validator: Validator[A], filter: RowFilter) extends UniqueJob(args) {

  Errors.safely(errors) {

    sources.map(p => TypedPipe.from[String](TextLine(p).read, 'line).map(l => s"$l$delimiter${timeSource.getTime(p)}"))
      .reduceLeft(_ ++ _)
      .map(line => Splitter.on(delimiter).split(line).asScala.toList)
      .flatMap(filter.run(_).toList)
      .map(record => Tag.tag[A](record).map({
        case (column, field) => clean.run(field, column)
       }))
      .map(record => Decode.decode[A](UnknownDecodeSource(record)))
      .map({
        case DecodeOk(value) =>
          validator.run(value).disjunction.leftMap(errors => s"""The following errors occured: ${errors.toList.mkString(",")}""")
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
