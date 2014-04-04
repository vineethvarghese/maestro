package au.com.cba.omnia.maestro.example

import au.com.cba.omnia.maestro.api._
import au.com.cba.omnia.maestro.example.thrift._
import com.twitter.scalding._
import au.com.cba.omnia.maestro.core.hive.TableDescriptor

class CustomerCascade(args: Args) extends Cascade(args) with MaestroSupport2[Customer, CustomerEnriched] {
  val domain        = "customer"
  val input         = s"/${env}/source/${domain}/*"
  val clean         = s"/${env}/processing/${domain}"
  val errors        = s"/${env}/errors/${domain}"

  // TODO: Clear up the naming around the Fields implicit using macro annotations, e.g: https://gist.github.com/quintona/9772837
  val byDate        = TableDescriptor(env, Partition.byDate(Fields1.EffectiveDate))
  val byCategory    = TableDescriptor(env, Partition.byFields1(Fields1.CustomerSubCat))
  val enriched      = TableDescriptor(env, Partition.byDate(Fields2.EffectiveDate))

  val cleaners      = Clean.all(
    Clean.trim,
    Clean.removeNonPrintables
  )
  val validators    = Validator.all(
    Validator.of(Fields1.CustomerSubCat, Check.oneOf("M", "F")),
    Validator.by[Customer](_.customerAcct.length == 3, "Customer accounts should always be a length of 3")
  )
  val filter        = RowFilter.keep

  val jobs = Guard.onlyProcessIfExists(input) { paths => List(
    maestro.load[Customer]("|", paths, clean, errors, maestro.now(), cleaners, validators, filter)
  , maestro.view(byDate, clean)
  , maestro.view(byCategory, clean)
  , maestro.hqlQuery("Convert Customer", List(byDate), enriched,
      s"INSERT OVERWRITE TABLE ${enriched.qualifiedName} select CustomerID,EffectiveDate from ${byDate.qualifiedName}")
  ) }
  
}
