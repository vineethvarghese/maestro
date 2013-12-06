package com.cba.maestro
package data

class DateDim(
  val date_sk: Int,
  val date_id: String,
  val date: String,
  val month_seq: Int,
  val week_seq: Int,
  val quarter_seq: Int,
  val year: Int,
  val dow: Int,
  val moy: Int,
  val dom: Int,
  val qoy: Int,
  val fy_year: Int,
  val fy_quarter_seq: Int,
  val fy_week_seq: Int,
  val day_name: String,
  val quarter_name: String,
  val holiday: String,
  val weekend: String,
  val following_holiday: String,
  val first_dom: Int,
  val last_dom: Int,
  val same_day_ly: Int,
  val same_day_lq: Int,
  val current_day: String,
  val current_week: String,
  val current_month: String,
  val current_quarter: String,
  val current_year: String
)
