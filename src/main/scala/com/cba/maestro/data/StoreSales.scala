package com.cba.maestro
package data

class StoreSales(
  val sold_date_sk: Int,
  val sold_time_sk: Int,
  val item_sk: Int,
  val customer_sk: Int,
  val cdemo_sk: Int,
  val hdemo_sk: Int,
  val addr_sk: Int,
  val store_sk: Int,
  val promo_sk: Int,
  val ticket_number: Int,
  val quantity: Int,
  val wholesale_cost: Double,
  val list_price: Double,
  val sales_price: Double,
  val ext_discount_amt: Double,
  val ext_sales_price: Double,
  val ext_wholesale_cost: Double,
  val ext_list_price: Double,
  val ext_tax: Double,
  val coupon_amt: Double,
  val net_paid: Double,
  val net_paid_inc_tax: Double,
  val net_profit: Double
)
