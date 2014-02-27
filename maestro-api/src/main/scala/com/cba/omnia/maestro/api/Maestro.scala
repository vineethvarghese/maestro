package com.cba.omnia.maestro.api

import com.cba.omnia.maestro.core.data._
import com.cba.omnia.maestro.core.partition._
import com.cba.omnia.maestro.core.codec._
import com.cba.omnia.maestro.core.task._
import com.twitter.scalding._, TDsl._
import com.twitter.scrooge._

case class Maestro(args: Args) {
  def view[A <: ThriftStruct : Decode : Manifest, B: Manifest: TupleSetter](partition: Partition[A, B], source: String, output: String) =
    View.create(args, partition, source, output)

  def load[A <: ThriftStruct : Decode : Manifest](delimiter: String, sources: List[String], output: String, errors: String, now: String) =
    Load.create(args, delimiter, sources, output, errors, now)
}
