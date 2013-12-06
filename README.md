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
   |  |dictionary|                              ||           |--> ad-hoc train + score
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


Components
----------

 - analytics platform:  Encompasses views, feature store, formats, and apis for getting
                        data in and out
 - tables / views:      Flat files stored on hdfs, with attached metadata, schema, lineage
 - feature store:       Just a set of tables / views which have been specifically designed
                        to be appropriate for use as a part of instance generation and
                        scoring
 - ad-hoc analytics:    Placeholder for search and reporting capacities over the analytics
                        platform.
 - daily orchestration: Coarse grained flow control, i.e. load, generate, score
 - job orchestration:   Scalding / cascading flow that combines jobs of (potentially) different
                        types into a coarse grained step. Jobs are split via domain and implementation,
                        i.e. commscore via sas, transactions via hive, logs via scalding....


Implementation Plan + Validation
--------------------------------

`[*] indicates validation step`

 - __simplifying assumptions__
     - sql via cascading will drop into place at some point in the future
     - text formats for first pass
 - __identify representitive data dimensions and obtain test data__
     - entity attributes (e.g. name, age, etc..)
     - entity * time data (e.g. transactions)
     - snapshot + delta of attributes
     - `[*]` _must_ be able to do impala/hive queries against time based dense data sets
     - `[*]` check viability of writing queries against sparse / delta feeds
 - __view derivation__
     - e.g. deltas into dense, querable, records
     - standard transformation types
     - some custom flows
     - `[*]` _must_ be able to do impala/hive queries against derived views
 - __transformations into feature store view__
     - scalding based
     - dsl based feature generation
     - incremental transformations (delta appended to t - 1, gives t)
     - joins across different data shapes
     - `[*]` _must_ be able to do impala/hive queries across feature store along instance dimension
     - `[*]` _must_ be able to do impala/hive queries across feature store along feature dimension
 - __generation and usage of feature dictionary__
     - `[*]` _must_ be able to do impala/hive queries across feature dictionary
     - `[*]` _must_ be able to use feature dictionary in data-science process (feeds into stats, normalization)
     - `[*]` _must_ be able to use hcatalog or similar to manage user metadata and associate with dictionary info
 - __instance generation / feature extraction__
     - standard extraction queries for sampling, partioning, joining etc....
     - custom extraction functions
 - __join feature extract with ad-hoc table / query__
     - `[*]` _must_ be able to effectively hive tables with productionised features
 - __extract for score__
     - `[*]` _must_ be able to perform score with efficient scan


Scaling implementation
----------------------

 - alternative storage formats
 - complete extraction api
 - complete feature generation dsl
 - complete standard load forms
 - incorporate work from cascading around running hive-ql
 - incorporate meta-data / lineage work
 - incorporate quality / stats work


Data Dimensions
---------------

Types and shapes of data landing on system.

 - Load type
     - delta (new records)
     - delta (changed records)
     - complete
 - Rate of change
     - static, e.g. slowly changing dimension, account codes
     - frequent, e.g. account balance
 - Cardinality
     - 1-1, direct attribute data, e.g. name, age, ...
     - 1-*, entity & time  keyed data, e.g. transactional data
 - Sparsity
     - Dense, i.e. all attributes have values
     - Sparse, i.e. optional attributes
 - Schema changes
 - Relationship
     - Direct, entity id as part of composite key
     - In-direct, secondary ids used, i.e. account id, other representations of customers identity
 - Orientation
     - Row oriented
     - Column oriented
 - Structure
     - delimited, with * delimiters
     - documents? not yet but maybe
     - header / trailer
     - control files
     - unstructured columns, i.e. web logs that may have composite data in a column
 - Landing
     -

Data Sets
---------


   <base>/load/
