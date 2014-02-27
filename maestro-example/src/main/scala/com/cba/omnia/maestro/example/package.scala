package com.cba.omnia.maestro

import com.cba.omnia.maestro.macros._
import com.cba.omnia.maestro.core.codec._
import com.cba.omnia.maestro.core.hdfs._
import com.cba.omnia.maestro.example.thrift._



package object example {
  lazy val yyyyMMdd = {
    val f = new java.text.SimpleDateFormat("yyyy-MM-dd")
    f.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))
    f
  }

  implicit def MaestroSupportDecode: Decode[Customer] =
    Macros.mkDecode[Customer]

  implicit def MaestroSupportEncode: Encode[Customer] =
    Macros.mkEncode[Customer]

  val Fields =
    Macros.mkFields[Customer]

}
