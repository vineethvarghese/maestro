package au.com.cba.omnia.maestro.core
package task

import java.util.UUID

import scala.collection.JavaConverters._
import scala.util.matching.Regex

import scalaz.{Tag => _, _}, Scalaz._

import com.google.common.base.Splitter

import cascading.flow.FlowDef

import com.twitter.scalding._, TDsl._

import com.twitter.scrooge.ThriftStruct

import au.com.cba.omnia.maestro.core.codec._
import au.com.cba.omnia.maestro.core.clean.Clean
import au.com.cba.omnia.maestro.core.validate.Validator
import au.com.cba.omnia.maestro.core.filter.RowFilter
import au.com.cba.omnia.maestro.core.scalding.Errors


sealed trait TimeSource {
  def getTime(path: String): String = this match {
    case Predetermined(time) => time
    case FromPath(extract)  => extract(path)
  }
}

case class Predetermined(time: String) extends TimeSource
case class FromPath(extract: String => String) extends TimeSource

object Load {
  def load[A <: ThriftStruct : Decode : Tag : Manifest]
    (delimiter: String, sources: List[String], errors: String, timeSource: TimeSource, clean: Clean,
      validator: Validator[A], filter: RowFilter)
    (implicit flowDef: FlowDef, mode: Mode): TypedPipe[A] = {
    loadProcess(
      sources.map(p => TextLine(p).map(l => s"$l$delimiter${timeSource.getTime(p)}")).reduceLeft(_ ++ _),
      delimiter,
      errors,
      clean,
      validator,
      filter
    )
  }

  /**
    * Append a UUID value to the end of each end. The last field of `A` needs to be set aside to
    * receive the uuid as type string.
    */
  def loadWithUUID[A <: ThriftStruct : Decode : Tag : Manifest]
    (delimiter: String, sources: List[String], errors: String, timeSource: TimeSource, clean: Clean,
      validator: Validator[A], filter: RowFilter)
    (implicit flowDef: FlowDef, mode: Mode): TypedPipe[A] = {
    val d = delimiter
    def uuid(): String = UUID.randomUUID.toString
    loadProcess(
      sources.map(p => TextLine(p).map(l => s"$l$d${timeSource.getTime(p)}$d${uuid()}")).reduceLeft(_ ++ _),
      delimiter,
      errors,
      clean,
      validator,
      filter
    )
  }

  def loadProcess[A <: ThriftStruct : Decode : Tag : Manifest]
    (in: TypedPipe[String], delimiter: String, errors: String, clean: Clean,
      validator: Validator[A], filter: RowFilter)
    (implicit flowDef: FlowDef, mode: Mode): TypedPipe[A] =
    Errors.safely(errors) {
      in
        .map(line => Splitter.on(delimiter).split(line).asScala.toList)
        .flatMap(filter.run(_).toList)
        .map(record =>
          Tag.tag[A](record).map { case (column, field) => clean.run(field, column) }
        ).map(record => Decode.decode[A](UnknownDecodeSource(record)))
        .map {
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
      }
    }
}
