package com.cba.maestro
package data

case class CustomerDemographics(
  demo_sk: Int,
  gender: String,
  marital_status: String,
  education_status: String,
  purchase_estimate: Int,
  credit_rating: String,
  dep_count: Int,
  dep_employed_count: Int,
  dep_college_count: Int
)
