package au.com.cba.omnia.maestro.core
package task


import au.com.cba.omnia.ebenezer.scrooge._
import au.com.cba.omnia.maestro.core.codec._
import au.com.cba.omnia.maestro.core.scalding._
import au.com.cba.omnia.maestro.core.partition._
import com.twitter.scalding._, TDsl._
import com.twitter.scrooge._
import au.com.cba.omnia.maestro.core.hive.Hive
import org.apache.hadoop.hive.conf.HiveConf

object View {
  def create[A <: ThriftStruct : Manifest, B: Manifest: TupleSetter](args: Args, partition: Partition[A, B], source: String, database: String, output: String) =
    new ViewJob(args, partition, source, database, output)
}

class ViewJob[A <: ThriftStruct : Manifest, B: Manifest: TupleSetter](args: Args, partition: Partition[A, B], source: String, database:String, output: String) extends UniqueJob(args) {

  //TODO: wrap this in IO or not? Context isn't clear in this case
  // How to error out the job?
  implicitly[Mode] match {
    case Hdfs(_, configuration) => {
      Hive.createHiveTap(new HiveConf,Hive.createDescriptor(database, partition), Hive.createScheme[A]).createResource(configuration)
    }
    case _ => true
  }

  ParquetScroogeSource[A](source)
     .map(v => partition.extract(v) -> v)
     .write(TemplateParquetScroogeSource[B, A](partition.pattern, output))

}
