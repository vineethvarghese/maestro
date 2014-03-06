package au.com.cba.omnia.maestro.core
package hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import com.twitter.scalding._

object Guard {
  def toProcess(path: String)(run: List[String] => List[Job]): List[Job] = {
    val conf = new Configuration
    val fs = FileSystem.get(conf)
    run(fs.globStatus(new Path(path))
      .toList
      .filter(s => fs.isDirectory(s.getPath))
      .map(_.getPath)
      .filter(p => !fs.exists(new Path(p, "_PROCESSED")))
      .map(_.toString))
  }
}
