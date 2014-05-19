package au.com.cba.omnia.maestro.core
package task

import cascading.flow.FlowDef

import com.twitter.scalding._, TDsl._

import com.twitter.scrooge.ThriftStruct

import au.com.cba.omnia.ebenezer.scrooge.TemplateParquetScroogeSource

import au.com.cba.omnia.maestro.core.partition.Partition

object View {
  def view[A <: ThriftStruct : Manifest, B: Manifest: TupleSetter]
    (partition: Partition[A, B], output: String)
    (pipe: TypedPipe[A])
    (implicit flowDef: FlowDef, mode: Mode): Unit = {
    pipe
      .map(v => partition.extract(v) -> v)
      .write(TemplateParquetScroogeSource[B, A](partition.pattern, output))
  }
}
