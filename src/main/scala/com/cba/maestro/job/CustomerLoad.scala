package com.cba.maestro
package job

import com.twitter.scalding._

class CustomerLoad(args: Args) extends Job(args) {
  TypedPipe.from[Row](TypedPsv[Row](args("input")), '*)
    .groupBy({ case (party, feature, value, date) => party })
    .sortBy({ case (party, feature, value, date) =>
      (party, date) })(DelayedPartyDateOrdering)
    .toTypedPipe
    .map({ case (_, (party, feature, value, date)) =>
      (buckets.bucketOf(party), party, feature, value, date) })
    .toPipe('bucket, 'party, 'feature, 'value, 'date)
    .write(TemplateCsv(args("output"), "%s", 'bucket, "|",
      ('party, 'feature, 'value, 'date)))
}
