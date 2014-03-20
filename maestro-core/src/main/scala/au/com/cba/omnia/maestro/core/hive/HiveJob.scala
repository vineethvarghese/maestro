package au.com.cba.omnia.maestro.core.hive

import com.twitter.scalding._
import cascading.flow.hive.HiveFlow 
import cascading.property.AppProps 
import au.com.cba.omnia.maestro.core.scalding._

object Hive {
  def create(args: Args, name : String, query: String, properties: Map[String, String] = Map.empty[String,String]) = new HiveJob(args,name,query)
}

class HiveJob(args: Args, name : String, query: String, properties: Map[String, String] = Map.empty[String,String]) extends UniqueJob(args){
  
  override def buildFlow = {
    val props: java.util.Properties = new java.util.Properties()
    props.put(AppProps.APP_ID, name)
    properties.map(t => props.put(t._1.asInstanceOf[Object],t._2.asInstanceOf[Object]))
    new HiveFlow(name, props, query)
  }

}
