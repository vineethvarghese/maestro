package au.com.cba.omnia.maestro.example

import au.com.cba.omnia.maestro.api._
import au.com.cba.omnia.maestro.core.codec._
import au.com.cba.omnia.maestro.example.thrift._
import com.twitter.scalding._
import com.twitter.scalding.filecache.DistributedCacheFile

class CustomerCascade(args: Args) extends Cascade(args) with MaestroSupport[Customer] {
  val maestro = Maestro(args)
 
  val env           = args("env")
  val domain        = "customer"
  val input         = s"${env}/source/${domain}/*"
  val clean         = s"${env}/processing/${domain}"
  val outbound      = s"${env}/outbound/${domain}"
  val analytics     = s"${env}/view/warehouse/${domain}"
  val errors        = s"${env}/errors/${domain}"
  val database      = OmniaUri(env, s"${domain}")
  val byDate        = database.table("by-date")
  val byCategory    = database.table("by-cat")
  val summary       = database.table("summary")
  val cleaners      = Clean.all(
    Clean.trim,
    Clean.removeNonPrintables
  )
  val validators    = Validator.all(
    Validator.of(Fields.CustomerSubCat, Check.oneOf("M", "F")),
    Validator.by[Customer](_.customerAcct.length == 4, "Customer accounts should always be a length of 4")
  )
  val filter        = RowFilter.keep
  
  DistributedCacheFile("/usr/local/lib/parquet-hive-bundle.jar")

  //Question: Move database name to the Maestro level? Trade off, what happens when you have mutliple DB cases, there will be many...
  def jobs = Guard.toProcess(input) { paths => List(
    maestro.load[Customer]("|", paths, clean, errors, maestro.now(), cleaners, validators, filter)
  , maestro.view(Partition.byDate(Fields.EffectiveDate), clean, database.databaseName, byDate)
  , maestro.view(Partition.byFields2(Fields.CustomerCat, Fields.CustomerSubCat), clean,database.databaseName, byCategory)
  , maestro.hql[Customer,Summary]("Create Summary", database.databaseName,s"insert into ${summary} select count(*) from ${byDate}")
  ) }
}
