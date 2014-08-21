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
package upload

import scala.util.control.NonFatal

import java.io.{File, FileOutputStream}

import java.util.zip.GZIPOutputStream

import com.google.common.io.Files

import scalaz.\/

/**
  * Operations on files
  *
  * TODO move these operations to the appropriate place in omnitool
  */
object FileOp {
  /**
    * Run an action on a temporary gzipped copy of a file.
    *
    * Typically the temporary file would be copied to other location(s)
    * during the action.
    *
    * The temporary file reference should not escape `action`.
    */
  def withTempCompressed[A](source: File, fileName: String, action: File => A): A = {
    // there must be a more convienient canonical way of doing this in scala ...
    val tempDir = Files.createTempDir
    val tempFile = new File(tempDir, fileName)
    try {
      val writer = new GZIPOutputStream(new FileOutputStream(tempFile))
      try {
        Files.copy(source, writer)
        writer.flush
      }
      finally {
        writer.close
      }
      action(tempFile)
    }
    finally {
      // assuming the tempFile reference did not escape action
      // and nobody else is doing anything with this file
      if (tempFile.exists && !tempFile.delete) {
        throw new Exception(s"can't delete temporary file $tempFile")
      }
      if (tempDir.exists && !tempDir.delete) {
        throw new Exception(s"can't delete temporary directory $tempDir")
      }
    }
  }
}
