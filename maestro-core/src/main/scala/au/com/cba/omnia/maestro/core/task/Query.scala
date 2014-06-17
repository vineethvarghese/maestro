package au.com.cba.omnia.maestro.core
package task

import hive._
import scalding._
import codec._
import au.com.cba.omnia.ebenezer.scrooge.hive._
import com.twitter.scalding._
import org.apache.hadoop.hive.conf.HiveConf
import au.com.cba.omnia.ebenezer.scrooge.hive.HiveJob
import cascading.flow.FlowDef
import partition._
import com.twitter.scrooge.ThriftStruct
import com.twitter.scalding.typed.EmptyTypedPipe

trait Query {
  // hqlQuery goes here
}
