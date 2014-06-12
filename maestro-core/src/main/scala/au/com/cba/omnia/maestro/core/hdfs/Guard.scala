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
package hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

case class GuardFilter(filter: (FileSystem, Path) => Boolean) {
  def &&&(that: GuardFilter): GuardFilter =
    GuardFilter((fs, p) => filter(fs, p) && that.filter(fs, p))
}

object Guard {
  /** Filter out any directories that HAVE a _PROCESSED file. */
  val NotProcessed = GuardFilter((fs, p) => !fs.exists(new Path(p, "_PROCESSED")))
  /** Filter out any directories that DO NOT HAVE a _INGESTION_COMPLETE file. */
  val IngestionComplete = GuardFilter((fs, p) => fs.exists(new Path(p, "_INGESTION_COMPLETE")))

  /** Expands the globs in the provided path and only keeps those directories that pass the filter. */
  def expandPaths(path: String, filter: GuardFilter = NotProcessed): List[String] = {
    val fs = FileSystem.get(new Configuration)
    fs.globStatus(new Path(path))
      .toList
      .filter(s => fs.isDirectory(s.getPath))
      .map(_.getPath)
      .filter(filter.filter(fs, _))
      .map(_.toString)
  }

  /** As `expandPath` but the filter is `NotProcessed` and `IngestionComplete`. */
  def expandTransferredPaths(path: String) = expandPaths(path, NotProcessed &&& IngestionComplete)
}
