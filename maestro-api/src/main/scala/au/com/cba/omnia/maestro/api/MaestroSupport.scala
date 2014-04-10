package au.com.cba.omnia.maestro.api

import scala.language.experimental.macros

import au.com.cba.omnia.maestro.core.codec._
import au.com.cba.omnia.maestro.macros._
import com.twitter.scrooge._

trait MaestroSupport[A <: ThriftStruct] extends MacroSupport[A]

trait MaestroSupport2[A <: ThriftStruct, B <: ThriftStruct] extends MacroSupport2[A, B]
