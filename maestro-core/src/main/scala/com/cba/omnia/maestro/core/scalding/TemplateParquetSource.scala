package com.cba.omnia.maestro.core
package scalding

import com.cba.omnia.ebenezer.scrooge._
import com.twitter.scalding._

import cascading.tap.SinkMode
import cascading.tap.Tap
import cascading.tap.hadoop.TemplateTap
import cascading.tap.hadoop.Hfs
import cascading.tuple.Tuple
import cascading.tuple.Fields

import com.twitter.scalding._, TDsl._


import cascading.scheme.Scheme
import cascading.tuple.Fields

import com.twitter.scalding._
import com.twitter.scrooge._

import org.apache.thrift._

import parquet.cascading._

// FIX this is nasty, and needs to be cleaned up and moved to ebenezer, most of the code should not be necessary, but it forceably works around scalding being broken
case class TemplateParquetScroogeSource[A, T <: ThriftStruct](template: String, p : String)(implicit m : Manifest[T], valueConverter: TupleConverter[T], valueSet: TupleSetter[T], ma : Manifest[A], partitionConverter: TupleConverter[A], partitionSet: TupleSetter[A])
  extends FixedPathSource(p)
  with TypedSink[(A, T)]
  with Mappable[(A, T)]
  with java.io.Serializable {

  // DO NOT USE intFields scalding / cascading Fields.merge is broken and gets called in bowels of TemplateTap.
  def toFields(start: Int, end: Int): Fields =
    Dsl.strFields((start until end).map(_.toString))

  override def hdfsScheme = {
    val scheme = HadoopSchemeInstance(new ParquetScroogeScheme[T].asInstanceOf[Scheme[_, _, _, _, _]])
    scheme.setSinkFields(toFields(0, 1))
    scheme
  }

  def templateFields =
    toFields(valueSet.arity, valueSet.arity + partitionSet.arity)

  override def sinkFields : Fields =
    toFields(0, valueSet.arity + partitionSet.arity)

  override def createTap(readOrWrite: AccessMode)(implicit mode : Mode): Tap[_,_,_] =
    (mode, readOrWrite) match {
      case (hdfsMode @ Hdfs(_, _), Read) =>
        createHdfsReadTap(hdfsMode)
      case (Hdfs(_, c), Write) =>
        val hfs = new Hfs(hdfsScheme, hdfsWritePath, SinkMode.UPDATE)
        new TemplateTap(hfs, template, templateFields)
      case (_, _) =>
        super.createTap(readOrWrite)(mode)
    }

  override def converter[U >: (A, T)] =
    TupleConverter.asSuperConverter[(A, T), U](new TupleConverter[(A, T)] {
      import cascading.tuple.TupleEntry

      def arity = valueConverter.arity + partitionConverter.arity

      def apply(te : TupleEntry) : (A, T) = {
        val value = new TupleEntry(toFields(0, valueConverter.arity))
        val partition = new TupleEntry(toFields(0, partitionConverter.arity))
        (0 until valueConverter.arity).foreach(idx => value.setObject(idx, te.getObject(idx)))
        (0 until partitionConverter.arity).foreach(idx => partition.setObject(idx, te.getObject(idx + valueConverter.arity)))
        partitionConverter(partition) -> valueConverter(value)
       }
    })

  override def setter[U <: (A, T)] =
    TupleSetter.asSubSetter[(A, T), U](new TupleSetter[(A, T)] {
      override def assertArityMatches(f : Fields) { /* there is a scalding bug that can't handle validation of arity properly if you are not outputing all fields */ }

      def arity = valueSet.arity + partitionSet.arity

      def apply(arg: (A, T)) = {
        val (a, t) = arg
        val partition = partitionSet(a)
        val value = valueSet(t)
        val output = Tuple.size(partition.size + value.size)
        (0 until value.size).foreach(idx => output.set(idx, value.getObject(idx)))
        (0 until partition.size).foreach(idx => output.set(idx + value.size, partition.getObject(idx)))
        output
      }
    })
}
