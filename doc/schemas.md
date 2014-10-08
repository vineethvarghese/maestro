
Schemas
=======

The maestro schema system provides utilities to infer table schemas from
sample data, and to check incoming data against those schemas.

The process for building a table schema consists of three steps:

1. Acquire a names file containing table column names and Hive storage types.
2. Scan through sample data to produce a classifier histogram for each column.
3. Using the names file and histogram, infer a skeleton table schema.
4. Manually inspect the resulting schema, and fill in missing types.
5. Check data against the schema.


Step 1. Name Acquisition
-----------------------

Suppose we have a table named 'accounts' that contains the opening and closing
date for a set of savings accounts. The first step is to build a names file
(`accounts.names`) that contains one line for each column, first word is the
column name and the second is the Hive storage type. For example:

```
account_name string
open_date    string
close_date   string
active       string
process_id   int
```

If there is an existing table in Hive, then the names file can be obtained
directly from the Hive metastore:

```
hive -e "describe accounts" > accounts.names
```


Step 2. Tasting
---------------

The "tasting" process takes some sample table data and produces a histogram
describing what sort of data appears in each column. This process produces a
new file, eg `accounts.taste`. Use the following job to do this:

```
java -cp ${SCHEMAS_JAR} com.twitter.scalding.Tool \
    au.com.cba.omnia.maestro.schema.commands.Taste \
    --hdfs \
    --in-local-names ${LOCAL_NAMES} \
    --in-hdfs-data   ${HDFS_DATA}   \
    --out-hdfs-taste ${HDFS_TASTE}  \
```

* `LOCAL_NAMES` local path to the file holding column names and hive storage types,
  eg the `accounts.names` file in the previous example.
* `HDFS_DATA`  hdfs path to either a single block of input data, or a
  directory that contains data blocks. 
* `HDFS_TASTE` hdfs path where the output taste file should be written. 

The resulting taste file is a JSON encoded list containing extracted metadata
for each column. For example:

```
{ "name"       : "eff_date", 
  "storage"    : "string", 
  "classifiers": { "Any": 185091, "Day.DDcMMcYYYY('.')": 185091 }, 
  "sample"     : { "maxSize": 100, "spilled": 0, 
                    "histogram": { "08.09.2014": 184790, "05.09.2014": 120, "04.09.2014": 18, 
                                   "02.09.2014": 10,     "29.08.2014": 4,   "03.09.2014": 4 } } 
}
```

The `name` and `storage` fields contain the information from the input names file.

For syntax names that include a period, such as `Day.DDcMMcYYYY('/')`, the
part before the period refers to a tope, which is a named entity in the world.
In this case the tope is `Day`, which is a day in the year. The part after the
period refers to the syntax used to represent the tope, in this case it is 
`DDcMMcYYYY('/')` which is a day written in day/month/year form with the parts
separated by forward slashes.

Syntax names that do not include a period to not necessarily refer to a tope
(named entity). For example, `White` matches to white space characters,
`AlphaNum` matches alphanumeric strings, `Upper` matches upper case
characters, and so on. Although the data that matches the `Upper` syntax may
have some more specific meaning, we are not always able to recognize it.


Step 3. Inference
-----------------

The inference process takes both the names and taste file and produces a table
schema. Use the following command:

```
java -cp ${SCHEMAS_JAR} au.com.cba.omnia.maestro.schema.commands.Infer \
    --database ${DATABASE_NAME} \
    --table    ${TABLE_NAME} \
    --names    ${LOCAL_NAMES} \
    --taste    ${LOCAL_TASTE} > ${LOCAL_SCHEMA}
```

 * `DATABASE_NAME` name of the database.
 * `TABLE_NAME`    name of the table.
 * `LOCAL_NAMES`   local path to names file produced in step 1 above. 
 * `LOCAL_TASTE`   local path to taste file produced in step 2 above.
 * `LOCAL_SCHEMA`  local path for output schema file.

Given the `accounts.names` and `account.taste` files above, we get the
following schema:

```
{ "name"     : "eff_date", 
  "storage"  : "string", 
  "format"   : "Day.DDcMMcYYYY('.')", 
  "histogram": { "Day.DDcMMcYYYY('.')": 185091 } 
}
```

The schema file contains one element for each column in the input table. 


Step 4. Fill in missing types
-----------------------------

TODO: say how to fill in missing types, or add new ones.


Step 5. Checking
----------------

```
hadoop jar ${SCHEMAS_JAR} com.twitter.scalding.Tool \
    au.com.cba.omnia.maestro.schema.jobs.Check \
    --hdfs \
    --in-schema       ${LOCAL_SCHEMA} \
    --in-hdfs-data    ${HDFS_DATA} \
    --out-hdfs-errors ${HDFS_ERRORS} \
    --out-hdfs-diag   ${HDFS_DIAG}
```

