
Schemas
=======

The maestro schema system provides utilities to infer table schemas from
sample data, and to check incoming data against those schemas.

The process for building a table schema consists of four steps:

1. Acquire a names file containing table column names and Hive storage types.
2. Scan through sample data to produce a classifier histogram for each column.
3. Using the names file and histogram, infer a skeleton table schema.
4. Manually inspect the resulting schema, and fill in missing types.

Table data can then be checked against the schema. Rows that do not match the
schema are written to an erroneous rows file, along with diagnostic information.


Step 1. Name Acquisition
------------------------

Suppose we have a table named 'accounts' that contains the opening and closing
dates for a set of savings accounts. The first step is to build a names file
(`accounts.names`) that contains one line for each column, where the first word
in each line is the column name and the second is the Hive storage type.
For example:

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

Trailing words on the line are ignored, such as field comments.


Step 2. Tasting
---------------

The "tasting" process reads sample table data and produces a histogram
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

* `LOCAL_NAMES` local path to the file holding column names and hive storage
  types, eg the `accounts.names` file in the previous example.
* `HDFS_DATA`  hdfs path to either a single block of input data, or a
  directory that contains data blocks. 
* `HDFS_TASTE` hdfs path where the output taste file should be written. 

The resulting taste file is a JSON encoded list containing extracted metadata
for each column. For example:

```
{ "name":        "eff_date", 
  "storage":     "string", 
  "classifiers": { "Any": 185091, "Day.DDcMMcYYYY('.')": 185091 }, 
  "sample": 
    { "maxSize": 100, "spilled": 0, 
      "histogram": { "08.09.2014": 184790, "05.09.2014": 120, "04.09.2014": 18, 
                     "02.09.2014": 10,     "29.08.2014": 4,   "03.09.2014": 4 } } 
}
```

The `name` and `storage` fields are the information from the input names file.

The classifiers field contains a set of tuples which record how many values in
the column match each of the known classifiers. The maestro schemas framework
includes a fixed set of classifiers, though it's easy to add new ones.

For classifiers whose names include a period, such as `Day.DDcMMcYYYYY('.')`, 
the part before the period (`Day`) refers to a tope, which is a named entity 
in the world. The part after the period (`DDcMMcYYYY('.')`) refers to the 
syntax used to represent the tope. In this case we have a physical Day being
represented as day, month and year numbers separated by periods, like  
"1965.05.02".

Classifiers whose names do not include a period are raw syntaxes that do not
nessesarally refer to a tope. For example, `White` matches to white space
characters, `AlphaNum` matches alphanumeric strings, `Upper` matches upper
case characters, and so on. Although the data that matches the `Upper` syntax
may have some more specific meaning, we are not always able to recognize it. 

The `Any` classifier matches all strings, so in the above example the
classifier set { "Any":185091, "Day.DDcMMcYYYY('.')":185091 } 


Step 3. Inference
-----------------

The inference process takes both the names and taste file and produces a
skeleton schema. Use the following command:

```
java -cp ${SCHEMAS_JAR} au.com.cba.omnia.maestro.schema.commands.Infer \
    --database ${DATABASE_NAME} \
    --table    ${TABLE_NAME}    \
    --names    ${LOCAL_NAMES}   \
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
{ "name"       : "eff_date", 
  "storage"    : "string", 
  "format"     : "Day.DDcMMcYYYY('.')", 
  "classifiers": { "Day.DDcMMcYYYY('.')": 185091 } 
}
```

The schema file contains one element for each column in the input table, where
the format field gives the inferred type for the column. The inference process
uses heuristics to choose a suitable format. In the above example it was easy
because all the values in the column were day strings.

TODO: add example with multiple classifiers.


Step 4. Fill in Missing Types
-----------------------------

If the inferencer heuristics were not able to infer a value type for a column
then the format field will be set to the place holder value "-". This will
happen if the values in a column match multiple classifiers, but no classifier
matches them all. This is usually caused by:

1. The column contains values that match an existing classifier, but the values
   should not have the associated type. For example, if a column contains the
   first names of people we may see a single 'AUD' (Audrey) value, but that
   same string can also be classified as a currency code (Australian Dollars).

2. The classifiers do not match the full range of values of the associated type.
   For example, if a classifier for financial system codes matched 
   'HLS' (home loan system) and 'SAV' (savings accounts), but the column also
   an unmatched code 'FOR' (foreign exchange).

3. The data is dirty or corrupted. For example, if most values in a value have
   the form YYYY.MM.DD, but there one that is '1900.01.'

In these cases the user needs to either manually update the schema with the
desired type, improve the classifier in the maestro implementation, or fix the 
input table in the case of corrupted data.

More details of the inference process are given in the [Inferencer Heuristics]
section.


Checking
--------

The final step is to check the data against the constructed table schema. Use
the following command:

```
hadoop jar ${SCHEMAS_JAR} com.twitter.scalding.Tool \
    au.com.cba.omnia.maestro.schema.jobs.Check \
    --hdfs \
    --in-schema       ${LOCAL_SCHEMA} \
    --in-hdfs-data    ${HDFS_DATA} \
    --out-hdfs-errors ${HDFS_ERRORS} \
    --out-hdfs-diag   ${HDFS_DIAG}
```

* `LOCAL_SCHEMA`  local path for input schema file.
* `HDFS_DATA`     hdfs path to either a single block of input data, or a
  directory that contains data blocks. This data is checked against the schema.
* `HDFS_ERRORS`   hdfs path to write a copy of the rows that do not match
  the schema.
* `HDFS_DIAG`     hdfs path to write error diagnostic information. For every 
  error row written to `HDFS_ERRORS`, we get a diagnostic row describing why
  the error row did not match the schema.


Inferencer heuristics
---------------------

 TODO: 

