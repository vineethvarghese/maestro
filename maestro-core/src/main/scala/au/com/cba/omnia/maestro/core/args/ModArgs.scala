//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package au.com.cba.omnia.maestro.core.args

import scala.reflect.ClassManifest // using Manifest rather than ClassTagfor thread safety

import scalaz._, Scalaz._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.CompressionCodec

import com.twitter.scalding.{Args, HadoopMode, Hdfs, Mode}

/**
  * Represents a set of changes to Scalding arguments.
  *
  * These changes can be applied to Scalding arguments safely:
  * producing a new modified set of arguments without mutating the original
  * arguments.
  *
  * We may have to do a deep copy of the Scalding arguments in order to safely
  * modify them, so it is best to apply one combined ModArgs value to Scalding
  * arguments, rather than applying many primitive ModArgs values.
  */
case class ModArgs(mutateConf: Option[Configuration => Unit], scaldingPairs: Map[String, Iterable[String]]) {
  /** Compress mapreduce output using specified codec */
  def compressOutput[C <: CompressionCodec : ClassManifest]: ModArgs =
    than(ModArgs.compressOutput[C])

  /** Set a key-value pair in the Scalding configuration */
  def set(key: String, value: String): ModArgs =
    than(ModArgs.set(key, value))

  /** Set a key-value pair in the underlying Hadoop configuration */
  def setHadoop(key: String, value: String): ModArgs =
    than(ModArgs.setHadoop(key, value))

  /** Run these modifications plus another set of modifications in sequence */
  def than(next: ModArgs): ModArgs =
    ModArgs(mutateConf |+| next.mutateConf, scaldingPairs ++ next.scaldingPairs)

  /** Take scalding arguments and safely return a modified copy */
  def modify(args: Args): Args =
    ModArgsImpl.modify(this, args)
}

/** Factory for producing ModArgs values */
object ModArgs {
  /** Set a key-value pair in the Scalding configuration */
  def set(key: String, value: String): ModArgs =
    ModArgs(None, Map((key, List(value))))

  /** Set a key-value pair in the underlying Hadoop configuration */
  def setHadoop(key: String, value: String): ModArgs =
    ModArgs(Some(_.set(key, value)), Map.empty)

  /** Compress mapreduce output using specified codec */
  def compressOutput[C <: CompressionCodec : ClassManifest]: ModArgs =
    ModArgs(Some(conf => {
      conf.set("mapred.output.compress",          "true")
      conf.set("mapred.output.compression.type",  "BLOCK")
      conf.set("mapred.output.compression.codec", implicitly[ClassManifest[C]].erasure.getName)
    }), Map.empty)
}

/** Utility methods on ModArgs which the user should not call themselves */
object ModArgsImpl {
  /** Returns a modified args without mutating original args */
  def modify(mod: ModArgs, args: Args): Args = {
    val withPairs = mod.scaldingPairs.foldLeft(args)(_+_)
    mod.mutateConf match {
      case Some(doMutateConf) => safelyApply(doMutateConf, withPairs)
      case None               => withPairs
    }
  }

  /** Mutate a copy of the hadoop configuration and return new args with mutated conf */
  def safelyApply(mutateConf: Configuration => Unit, args: Args): Args = Mode.getMode(args) match {
    // Scalding args by themselves are immutable. Only the Mode may have mutable parts
    case None => args

    // The Hadoop Configuration stored in a Hdfs mode is mutable and must be cloned
    case Some(hdfs : Hdfs) => {
      val newConf = new Configuration(hdfs.conf)
      mutateConf(newConf)
      Mode.putMode(hdfs.copy(conf = newConf), args)
    }

    // Some other modes have mutable parts,
    // but those parts are private and it's not trivial to access and clone them
    // we don't currently use these Modes, so I won't worry about them for now
    case Some(mode) => {
      throw new Exception(s"ModArgs doesn't yet know how to safely modify this mode: $mode")
    }
  }
}
