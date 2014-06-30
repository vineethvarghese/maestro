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

package au.com.cba.omnia.maestro.core

package task

import com.twitter.scalding.{ Args, Job, Source, TextLine, TypedPipe, TypedTsv }

import org.apache.hadoop.fs.Path

import org.specs2.execute.{Failure, FailureException}

import scalaz.Validation

import au.com.cba.omnia.thermometer.core.{ThermometerSpec, ThermometerSource}
import au.com.cba.omnia.thermometer.context.Context
import au.com.cba.omnia.thermometer.tools.Jobs

import au.com.cba.omnia.maestro.core.clean.Clean
import au.com.cba.omnia.maestro.core.codec.{Decode, Tag}
import au.com.cba.omnia.maestro.core.data.Field
import au.com.cba.omnia.maestro.core.filter.RowFilter
import au.com.cba.omnia.maestro.core.split.Splitter
import au.com.cba.omnia.maestro.core.validate.Validator
import au.com.cba.omnia.maestro.core.thrift.scrooge.StringPair

object LoadSpec extends ThermometerSpec { def is = s2"""

Load properties
===============

loadProcess applies splitter correctly $loadProcessSplits

"""

  def loadProcessSplits = {

    val testJob = new LoadProcessSplitsJob(scaldingArgs)
    val result = Jobs.runJob(testJob)

    result match {
      case None => true
      case Some(err) => {
        err.fold(println(_), _.printStackTrace())
        false
      }
    }
  }
}

object LoadLogic extends Load

class LoadProcessSplitsJob(args: Args) extends Job(args) {

  implicit val StringPairDecode: Decode[StringPair] = for {
    first <- Decode.of[String]
    second <- Decode.of[String]
  } yield StringPair(first, second)

  implicit val StringPairTag: Tag[StringPair] = {
    val fields =
      Field("FIRST", (p:StringPair) => p.first) +:
      Field("SECOND",(p:StringPair) => p.second) +:
      Stream.continually[Field[StringPair,String]](
        Field("UNKNOWN", _ => throw new Exception("invalid field"))
      )

    Tag(_ zip fields)
  }

  val in = ThermometerSource(List("col1$col02", "colA$col_B", "colI$colII"))
  val splitter = Splitter.delimited("$")
  val clean = Clean((_, row) => row)
  val validator = Validator[StringPair](Validation.success)
  val filter = RowFilter.keep
  val outputPath = "output"
  val out = TypedTsv[String](outputPath)

  LoadLogic.loadProcess[StringPair](in, splitter, None, clean, validator, filter)
    .map(t => if (true) t.first + "$" + t.second // TODO will check fixed length splitting
              else throw new Exception(s"bad split result: $t"))
    .write(out)
}
