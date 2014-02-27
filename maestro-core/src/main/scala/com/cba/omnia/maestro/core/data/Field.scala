package com.cba.omnia.maestro.core
package data

case class Field[A, B](value: String, get: A => B)
