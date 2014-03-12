package au.com.cba.omnia.maestro.core
package task

import au.com.cba.omnia.ebenezer.scrooge._
import au.com.cba.omnia.maestro.core.scalding._
import au.com.cba.omnia.maestro.core.codec._
import au.com.cba.omnia.maestro.core.clean._
import au.com.cba.omnia.maestro.core.validate._
import au.com.cba.omnia.maestro.core.filter._

import com.google.common.base.Splitter
import com.twitter.scalding._, TDsl._
import com.twitter.scrooge._

import scala.collection.JavaConverters._
import scalaz.{Tag => _, _}, Scalaz._

object Load {
  def create[A <: ThriftStruct : Decode : Tag : Manifest](args: Args, delimiter: String, sources: List[String], output: String, errors: String, now: String, clean: Clean, validator: Validator[A], filter: RowFilter) =
    new LoadJob(args, delimiter, sources, output, errors, now, clean, validator, filter)
}

class LoadJob[A <: ThriftStruct : Decode: Tag : Manifest](args: Args, delimiter: String, sources: List[String], output: String, errors: String, now: String, clean: Clean, validator: Validator[A], filter: RowFilter) extends UniqueJob(args) {

  Errors.safely(errors) {

    sources.map(p => TextLine(p).read).reduceLeft(RichPipe(_) ++ RichPipe(_))
      .toTypedPipe[String]('line)
      .map(line => Splitter.on(delimiter).split(line).asScala.toList)
      .flatMap(filter.run(_).toList)
      .map(record => Tag.tag[A](record).map({
        case (column, field) => clean.run(field, column)
       }))
      .map(record => Decode.decode[A](UnknownDecodeSource(record ++ List(now))))
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
