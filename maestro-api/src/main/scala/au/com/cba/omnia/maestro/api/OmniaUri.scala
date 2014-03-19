package au.com.cba.omnia.maestro.example


case class OmniaUri(environment: String, database: String){
  def table(tbl:String) = {s"${environment}/view/warehouse/${database}/${tbl}"}
  def databaseName = s"${environment}_${database}"
}
