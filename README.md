maestro
=======

```
maestro: a distinguished conductor
```


overview
--------

```
                        ad-hoc analytics

                              ^^
                              ||
   +---------------------------------------------------------+
   |                  --------------   +-- feature store --+ |
   |  ------------  / | derived q1 |   |                   | |
   |  |attributes| <  --------------   |[ inst | features ]| |
   |  ------------  \ --------------   |[ inst | features ]|----> feature extraction /
   |                  | derived q2 |   |[ inst | features ]|----> instance generation
   |  ------------    --------------   |[ inst | features ]| |
   |  |    tx    |                     |                   | |
   |  ------------                     +-------------------+ |
   |                                            ||     |     |
   |  ------------                              ||     ---------> experimentation &
   |  |dictionary|                              ||           |--> ad-hoc train + core
   |  ------------                              ||           |
   +--------------------------------------------||-----------+
                                                ||
                                                vv
                                              scoring


```

Phases
------

 - data loads
 - view generation
 - experimentation
 - production training
 - scoring


Implementation Plan + Validation
--------------------------------

 - simplifying assumptions
  - sql via cascading will drop into place at some point in the future
  - text formats for first pass
 - representitive data shapes
  - entity attributes (e.g. name, age, etc..)
  - entity * time data (e.g. transactions)
  - snapshot + delta of attributes
  - [*] _must_ be able to do impala/hive queries against time based dense data sets
  - [*] check viability of writing queries against sparse / delta feeds
 - view derivation
  - e.g. deltas into dense, querable, records
  - [*] _must_ be able to do impala/hive queries against derived views
 - transformations into feature store view
  - scalding based
  - dsl based feature generation
  - incremental transformations (delta appended to t - 1, gives t)
  - joins across different data shapes
  - [*] _must_ be able to do impala/hive queries across feature store along instance dimension
  - [*] _must_ be able to do impala/hive queries across feature store along feature dimension
 - generation and usage of feature dictionary
  - [*] _must_ be able to do impala/hive queries across feature dictionary
  - [*] _must_ be able to use feature dictionary in data-science process (feeds into stats, normalization)
  - [*] _must_ be able to use hcatalog or similar to manage user metadata and associate with dictionary info
 - instance generation / feature extraction
 - join feature extract with ad-hoc table / query
 - extract for score


Scaling implementation
----------------------

 - alternative storage formats
 - complete extraction api
 - complete feature generation dsl
 - complete standard load forms
 - incorporate work from cascading around running hive-ql
 - incorporate meta-data / lineage work
 - incorporate quality / stats work
