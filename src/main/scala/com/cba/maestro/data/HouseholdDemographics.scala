package com.cba.maestro
package data

case class HouseholdDemographics(
  demo_sk: Int,
  income_band_sk: Int,
  buy_potential: String,
  dep_count: Int,
  vehicle_count: Int
)
