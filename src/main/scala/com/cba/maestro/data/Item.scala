package com.cba.maestro
package data

case class Item(
  item_sk: Int,
  item_id: String,
  rec_start_date: String,
  rec_end_date: String,
  item_desc: String,
  current_price: Double,
  wholesale_cost: Double,
  brand_id: Int,
  brand: String,
  class_id: Int,
  clazz: String,
  category_id: Int,
  category: String,
  manufact_id: Int,
  manufact: String,
  size: String,
  formulation: String,
  color: String,
  units: String,
  container: String,
  manager_id: Int,
  product_name: String
)
