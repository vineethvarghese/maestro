package au.com.cba.omnia.maestro.example

import au.com.cba.omnia.maestro.api._
import au.com.cba.omnia.maestro.core.codec._
import au.com.cba.omnia.maestro.example.thrift._
import com.twitter.scalding._

class CustomerCascade(args: Args) extends Cascade(args) with MaestroSupport[Customer] {
  val maestro = Maestro(args)

  val env           = args("env")
  val domain        = "customer"
  val input         = s"${env}/source/${domain}/*"
  val clean         = s"${env}/processing/${domain}"
  val outbound      = s"${env}/outbound/${domain}"
  val analytics     = s"${env}/view/warehouse/${domain}"
  val dateView      = s"${env}/view/warehouse/${domain}/by-date"
  val catView       = s"${env}/view/warehouse/${domain}/by-cat"
  val errors        = s"${env}/errors/${domain}"
  val cleaners      = Clean.all(
    Clean.trim,
    Clean.removeNonPrintables
  )
  val validators    = Validator.all(
    Validator.of(Fields.CustomerSubCat, Check.oneOf("M", "F")),
    Validator.by[Customer](_.customerAcct.length == 3, "Customer accounts should always be a length of 3")
  )
  val filter        = RowFilter.keep

  def jobs = Guard.toProcess(input) { paths => List(
    maestro.load[Customer]("|", paths, clean, errors, maestro.now(), cleaners, validators, filter)
  , maestro.view(Partition.byDate(Fields.EffectiveDate), clean, dateView)
  , maestro.view(Partition.byFields2(Fields.CustomerCat, Fields.CustomerSubCat), clean, catView)
  ) }

  override def validate { /* workaround for scalding bug, yep, yet another one, no nothing works */ }
}
