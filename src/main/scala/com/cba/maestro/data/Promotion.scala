package com.cba.maestro
package data

case class Promotion(
  promo_sk: Int,
  promo_id: String,
  start_date_sk: Int,
  end_date_sk: Int,
  item_sk: Int,
  cost: Double,
  response_target: Int,
  promo_name: String,
  channel_dmail: String,
  channel_email: String,
  channel_catalog: String,
  channel_tv: String,
  channel_radio: String,
  channel_press: String,
  channel_event: String,
  channel_demo: String,
  channel_details: String,
  purpose: String,
  discount_active: String
)
