package au.com.cba.omnia.maestro.core
package task


import au.com.cba.omnia.ebenezer.scrooge._
import au.com.cba.omnia.maestro.core.codec._
import au.com.cba.omnia.maestro.core.scalding._
import au.com.cba.omnia.maestro.core.partition._
import com.twitter.scalding._, TDsl._
import com.twitter.scrooge._
import au.com.cba.omnia.maestro.core.hive.{TableDescriptor, Hive}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.mapred.JobConf

object View {
  def create[A <: ThriftStruct : Manifest, B: Manifest: TupleSetter](args: Args, descriptor: TableDescriptor[A,B], source: String) =
    new ViewJob(args, descriptor, source)
}

class ViewJob[A <: ThriftStruct : Manifest, B: Manifest: TupleSetter](args: Args, descriptor: TableDescriptor[A,B], source: String) extends UniqueJob(args) {

  //TODO: wrap this in IO or not? Context isn't clear in this case
  // How to error out the job?
  implicitly[Mode] match {
    case Hdfs(_, configuration:JobConf) => {
      Hive.createHiveTap(new HiveConf,descriptor.createHiveDescriptor(), descriptor.createScheme()).createResource(configuration)
    }
    case _ => true
  }

  ParquetScroogeSource[A](source)
     .map(v => descriptor.partition.extract(v) -> v)
     .write(TemplateParquetScroogeSource[B, A](descriptor.partition.pattern, descriptor.tablePath))

}
