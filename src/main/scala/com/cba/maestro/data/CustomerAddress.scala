package com.cba.maestro
package data

case class CustomerAddress(
  address_sk: Int,
  address_id: String,
  street_number: String,
  street_name: String,
  street_type: String,
  suite_number: String,
  city: String,
  county: String,
  state: String,
  zip: String,
  country: String,
  gmt_offset: Double,
  location_type: String
)
