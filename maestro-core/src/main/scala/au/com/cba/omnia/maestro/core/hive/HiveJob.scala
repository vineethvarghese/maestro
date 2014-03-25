package au.com.cba.omnia.maestro.core.hive

import com.twitter.scalding._
import au.com.cba.omnia.maestro.core.scalding._
import org.apache.hadoop.hive.conf.HiveConf
import cascading.scheme.Scheme
import cascading.flow.hive.HiveFlow
import cascading.tap.hive.{HiveTap, HiveTableDescriptor}
import cascading.tap.{Tap, SinkMode}
import au.com.cba.omnia.ebenezer.scrooge.ParquetScroogeScheme
import org.apache.hadoop.mapred.{OutputCollector, RecordReader, JobConf}

object Hive {
  def create(args: Args, name: String, query: String, inputDescriptors: List[TableDescriptor[_,_]], outputDescriptor: TableDescriptor[_,_], hiveConf: HiveConf) = {
    val inputTaps = inputDescriptors.map(d => CastHfsTap(createHiveTap(hiveConf, d.createHiveDescriptor(), d.createScheme())))
    val outputTap = CastHfsTap(createHiveTap(hiveConf, outputDescriptor.createHiveDescriptor(), outputDescriptor.createScheme()))
    new HiveJob(args,name,query, inputTaps, outputTap)
  }

  // Scala is pickier than Java about type parameters, and Cascading's Scheme
  // declaration leaves some type parameters underspecified.  Fill in the type
  // parameters with wildcards so the Scala compiler doesn't complain.
  // Leaves a bad taste in the mouth
  object HadoopSchemeFromParquetScheme {
    def apply(scheme: ParquetScroogeScheme[_]) =
      scheme.asInstanceOf[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]]
  }


  def createHiveTap(conf : HiveConf, descriptor : HiveTableDescriptor, scheme: ParquetScroogeScheme[_]) = {
    new HiveTap(conf, descriptor, HadoopSchemeFromParquetScheme(scheme), SinkMode.REPLACE, true)
  }
}

class HiveJob(args: Args, name : String, query: String, inputTaps: List[Tap[_, _, _]], outputTap: Tap[_, _, _]) extends UniqueJob(args){

  import collection.JavaConversions._

  override def buildFlow = {
    new HiveFlow(name, query, inputTaps.to, outputTap)
  }

}
