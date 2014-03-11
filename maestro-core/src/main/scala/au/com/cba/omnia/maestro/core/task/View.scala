package au.com.cba.omnia.maestro.core
package task


import au.com.cba.omnia.ebenezer.scrooge._
import au.com.cba.omnia.maestro.core.codec._
import au.com.cba.omnia.maestro.core.scalding._
import au.com.cba.omnia.maestro.core.partition._
import com.twitter.scalding._, TDsl._
import com.twitter.scrooge._

object View {
  def create[A <: ThriftStruct : Manifest, B: Manifest: TupleSetter](args: Args, partition: Partition[A, B], source: String, output: String) =
    new ViewJob(args, partition, source, output)
}

class ViewJob[A <: ThriftStruct : Manifest, B: Manifest: TupleSetter](args: Args, partition: Partition[A, B], source: String, output: String) extends UniqueJob(args) {

   ParquetScroogeSource[A](source)
     .map(v => partition.extract(v) -> v)
     .write(TemplateParquetScroogeSource[B, A](partition.pattern, output))

}
