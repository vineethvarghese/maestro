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
package split

import java.nio.ByteBuffer
import java.security.{MessageDigest, SecureRandom}

import scala.util.hashing.MurmurHash3
import scala.collection.JavaConverters._

import com.google.common.base.{Splitter => GoogSplitter}

import cascading.flow.{FlowDef, FlowProcess}
import cascading.tap.hadoop.io.MultiInputSplit.CASCADING_SOURCE_PATH

import au.com.cba.omnia.maestro.core.task._//TimeSource

object Other {
  /** Creates a unique key from the provided information.*/
  def uid(path: String, slice: Int, offset: Long, line: String, pathHashes: Map[String, String]): String = {
    val md         = MessageDigest.getInstance("SHA-1")
    val byteBuffer = ByteBuffer.allocate(12)
    val hash       = pathHashes(path)
    val lineHash   = md.digest(line.getBytes("UTF-8")).drop(8)

    val lineInfo   = byteBuffer.putInt(slice).putLong(offset).array
    val hex        = (lineInfo ++ lineHash).map("%02x".format(_)).mkString

    s"$hash$hex"
  }

  def getSourcePath(flowProcess: FlowProcess[_]): String = 
    flowProcess.getProperty(CASCADING_SOURCE_PATH).toString
}

// TODO add validation
case class Chipper(run: (String, Long, FlowProcess[_]) => List[String]) {
  def join(other: Chipper): Chipper =
    Chipper((l, offset, fp) => this.run(l, offset, fp) ++ other.run(l, offset, fp))
}

object Chipper {
  def delimited(delimiter: String) =
    Chipper((l, _, _) => GoogSplitter.on(delimiter).split(l).asScala.toList)

  def fixed(lengths: List[Int]): Chipper = ???

  def uniqueKey(inputs: List[String]) = {
    val rnd    = new SecureRandom()
    val seed   = rnd.generateSeed(4)
    val md     = MessageDigest.getInstance("SHA-1")
    val pathHashes =
      inputs
        .map(k => k -> md.digest(seed ++ k.getBytes("UTF-8")).drop(12).map("%02x".format(_)).mkString)
        .toMap

    Chipper((l, offset, fp) => {
      val md   = MessageDigest.getInstance("SHA-1")
      val key = Other.uid(
        Other.getSourcePath(fp),
        fp.getCurrentSliceNum,
        offset,
        l,
        pathHashes
      )

      List(key)
    })
  }

  def time(timeSource: TimeSource) =
    Chipper((_, _, fp) => List(timeSource.getTime(Other.getSourcePath(fp))))

  def field(field: String*) =
    Chipper((_, _, _) => field.toList)
}

object Test {
  Chipper.delimited("|").join(Chipper.time(Predetermined("aa")))
}
