package com.cba.maestro
package data

case class Customer(
  customer_sk: Int,
  customer_id: String,
  current_cdemo_sk: Int,
  current_hdemo_sk: Int,
  current_addr_sk: Int,
  first_shipto_date_sk: Int,
  first_sales_date_sk: Int,
  salutation: String,
  first_name: String,
  last_name: String,
  preferred_cust_flag: String,
  birth_day: Int,
  birth_month: Int,
  birth_year: Int,
  birth_country: String,
  login: String,
  email_address: String,
  last_review_date: String
)
