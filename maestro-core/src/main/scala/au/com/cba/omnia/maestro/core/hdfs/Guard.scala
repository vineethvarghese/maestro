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
