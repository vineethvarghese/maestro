package au.com.cba.omnia.maestro.api

import scala.util.matching.Regex

import cascading.flow.FlowDef

import com.twitter.scalding._, TDsl._

import com.twitter.scrooge.ThriftStruct

import au.com.cba.omnia.maestro.core.codec.{Decode, Tag}
import au.com.cba.omnia.maestro.core.task._
import au.com.cba.omnia.maestro.core.scalding.UnravelPipeImplicits

import au.com.cba.omnia.maestro.macros.SplitMacro

object Maestro extends UnravelPipeImplicits {
  /**
    * Loads the supplied files, filters, cleans, validates and parses the rows into
    * the specified ThriftStruct. It also appends the time using the specified time source to the
    * struct.
    */
  def load[A <: ThriftStruct : Decode : Tag : Manifest]
    (delimiter: String, sources: List[String], errors: String, timeSource: TimeSource, clean: Clean,
      validator: Validator[A], filter: RowFilter)
    (implicit flowDef: FlowDef, mode: Mode): TypedPipe[A] =
    Load.load(delimiter, sources, errors, timeSource, clean, validator, filter)

  /** Partitions a pipe using the given partition scheme and writes out the data.*/
  def view[A <: ThriftStruct : Manifest, B: Manifest: TupleSetter]
    (partition: Partition[A, B], output: String)
    (pipe: TypedPipe[A])
    (implicit flowDef: FlowDef, mode: Mode): Unit =
    View.view(partition, output)(pipe)
  
  /**
    * Splits the given struct A into a tuple of smaller thrift structs by matching the field names.
    * 
    * It can duplicate the same original field across several structs, rearrange field order and skip fields.
    */
  def split[A <: ThriftStruct, B <: Product]: A => B =
    macro SplitMacro.impl[A, B]

  /** Use the current time yyyy-MM-dd as the load time for the data */
  def now(format: String = "yyyy-MM-dd") = {
    val f = new java.text.SimpleDateFormat(format)
    f.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))
    Predetermined(f.format(new java.util.Date))
  }

  /**
    * Derive the load time from the file path using the provided regex.
    * The regex needs to extract the year,month and day into separate capture groups.
    */
  def timeFromPath(regex: Regex): TimeSource = FromPath { (path: String) =>
    val m = regex.pattern.matcher(path)
    m.matches
    s"${m.group(1)}-${m.group(2)}-${m.group(3)}"
  }
}
