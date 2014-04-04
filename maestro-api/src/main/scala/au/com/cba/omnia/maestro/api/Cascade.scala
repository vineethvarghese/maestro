package au.com.cba.omnia.maestro.api

import java.io.File._
import com.twitter.scalding._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import au.com.cba.omnia.ebenezer.introspect.ParquetIntrospectTools._
import au.com.cba.omnia.ebenezer.introspect.Record
import Maestro._
import scala.collection._
import com.twitter.scalding.filecache.DistributedCacheFile

abstract class Cascade(args: Args) extends CascadeJob(args) {

  /**
    * The environment.
    * Used for file paths.
    */
  final val env: String = args("env")

  /**
    * A maestro value to create jobs with.
    */
  final val maestro = Maestro(env, args)
 
  override def validate { /* workaround for scalding bug, yep, yet another one, no nothing works */ }
}

