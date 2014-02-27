package com.cba.omnia.maestro.api

import scala.language.experimental.macros

import com.cba.omnia.maestro.core.codec._
import com.cba.omnia.maestro.macros._
import com.twitter.scrooge._

trait MaestroSupport[A <: ThriftStruct] extends MacroSupport[A]
