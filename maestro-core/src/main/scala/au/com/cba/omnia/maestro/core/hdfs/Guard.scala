package au.com.cba.omnia.maestro.core
package hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object Guard {
  def expandPaths(path: String): List[String] = {
    val fs = FileSystem.get(new Configuration)
    fs.globStatus(new Path(path))
      .toList
      .filter(s => fs.isDirectory(s.getPath))
      .map(_.getPath)
      .filter(p => !fs.exists(new Path(p, "_PROCESSED")))
      .map(_.toString)
  }
}
