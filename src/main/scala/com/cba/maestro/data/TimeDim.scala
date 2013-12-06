package com.cba.maestro
package data

case class TimeDim(
  time_sk: Int,
  time_id: String,
  time: Int,
  hour: Int,
  minute: Int,
  second: Int,
  am_pm: String,
  shift: String,
  sub_shift: String,
  meal_time: String
)
