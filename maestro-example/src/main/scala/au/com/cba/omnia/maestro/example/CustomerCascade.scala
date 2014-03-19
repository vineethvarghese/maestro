package au.com.cba.omnia.maestro.example

import au.com.cba.omnia.maestro.api._
import au.com.cba.omnia.maestro.core.codec._
import au.com.cba.omnia.maestro.example.thrift._
import com.twitter.scalding._
import com.twitter.scalding.filecache.DistributedCacheFile

class OpsCascade(args: Args) extends Cascade(args){
  val maestro       = Maestro(args)
  val env           = args("env")                                                                                                                                                                                                                                                 
  val domain        = "customer"                                                                                                                                                                                                                                                  
  val database      = OmniaUri(env, s"${domain}")
  val db            = database.databaseName

  DistributedCacheFile("/usr/local/lib/parquet-hive-bundle.jar")

  val storageFormat = "ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'" +
    "STORED AS" + 
    "INPUTFORMAT 'parquet.hive.DeprecatedParquetInputFormat'"+
    "OUTPUTFORMAT 'parquet.hive.DeprecatedParquetOutputFormat';"

  def jobs = List(
    maestro.hql(s"create DATABASE IF NOT EXISTS ${db}")
   ,maestro.hql(s"create TABLE IF NOT EXISTS ${db}.by-date(CUSTOMER_ID string, " + 
     "CUSTOMER_NAME string, " +
     "CUSTOMER_ACCT string, " +
     "CUSTOMER_SUB_CAT string," +  
     "CUSTOMER_BALANCE string," +  
     "EFFECTIVE_DATE string) PARTITIONED BY(EFFECTIVE_DATE) " + storageFormat)
   ,maestro.hql(s"create TABLE IF NOT EXISTS ${db}.by-date(CUSTOMER_ID string, " +                                                                                                                                                                                                      "CUSTOMER_NAME string, " +                                                                                                                                                                                                                                                        "CUSTOMER_ACCT string, " +                                                                                                                                                                                                                                                        "CUSTOMER_SUB_CAT string," +                                                                                                                                                                                                                                                      "CUSTOMER_BALANCE string," +                                                                                                                                                                                                                                                      "EFFECTIVE_DATE string) PARTITIONED BY(CUSTOMER_SUB_CAT) " + storageFormat) 
   ,maestro.hql(s"create TABLE IF NOT EXISTS ${db}.summary(count int) " + storageFormat)
  )
}

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
  

  def jobs = Guard.toProcess(input) { paths => List(
    maestro.load[Customer]("|", paths, clean, errors, maestro.now(), cleaners, validators, filter)
  , maestro.view(Partition.byDate(Fields.EffectiveDate), clean, byDate)
  , maestro.view(Partition.byFields2(Fields.CustomerCat, Fields.CustomerSubCat), clean, byCategory)
  , maestro.hql(s"insert into ${summary} select count(*) from ${byDate}")
  ) }

  override def validate { /* workaround for scalding bug, yep, yet another one, no nothing works */ }
}
