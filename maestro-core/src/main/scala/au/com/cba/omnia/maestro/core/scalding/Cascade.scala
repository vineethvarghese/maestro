package au.com.cba.omnia.maestro.core
package scalding

import com.twitter.scalding._
import com.twitter.scalding.filecache.DistributedCacheFile

abstract class Cascade(args: Args) extends CascadeJob(args) {
  DistributedCacheFile("/usr/local/lib/parquet-hive-bundle.jar")
  override def validate { /* workaround for scalding bug, yep, yet another one, no nothing works */ }
}
