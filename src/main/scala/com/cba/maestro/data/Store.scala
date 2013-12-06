package com.cba.maestro
package data

class Store(
  val store_sk: Int,
  val store_id: String,
  val rec_start_date: String,
  val rec_end_date: String,
  val closed_date_sk: Int,
  val store_name: String,
  val number_employees: Int,
  val floor_space: Int,
  val hours: String,
  val manager: String,
  val market_id: Int,
  val geography_class: String,
  val market_desc: String,
  val market_manager: String,
  val division_id: Int,
  val division_name: String,
  val company_id: Int,
  val company_name: String,
  val street_number: String,
  val street_name: String,
  val street_type: String,
  val suite_number: String,
  val city: String,
  val county: String,
  val state: String,
  val zip: String,
  val country: String,
  val gmt_offset: Double,
  val tax_precentage: Double
)
