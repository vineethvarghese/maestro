
Schemas
=======

The maestro schema system provides utilities to infer table schemas from
sample data, and to check incoming data against those schemas.

The process for building a table schema consists of three steps:

1. Acquire a names file containing table column names and Hive storage types.
2. Scan through sample data to produce a syntax histogram for each column.
3. Using the names file and histogram, infer a skeleton table schema.
4. Manually inspect the resulting schema, and fill in missing types.


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
java -cp ${MAESTRO_JAR} au.com.cba.omnia.maestro.schema.commands.Taste \
    --input <hdfs_input_path> --output <hdfs_taste_file>
```

* `hdfs_input_path` hdfs path to either a single block of input data, or a
  directory that contains data blocks. 
* `hdfs_taste_file` hdfs path where the output histogram should be written. 

An example histogram is as follows:

```
Any:100000, White:9, AlphaNum:99991;
Any:100000, White:1023, Day.DDcMMcYYYY('/'):98977;
Any:100000, Day.DDcMMcYYYY('/'):100000;
Any:100000, AlphaNum:100000, Alpha:100000, Upper:100000, Exact("Y"):80123,
            Exact("N"):19877;
Any:100000, AlphaNum:100000, Real:100000, Int:100000, Nat:100000; 
```

In the histogram we get one line for each column input table. Each line then
contains a comma separated list of syntax names, along with how many values in
the column matched each syntax. 

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
java -cp ${MAESTRO_JAR} au.com.cba.omnia.maestro.schema.commands.Infer \
    --database ${DATABASE_NAME} --table ${TABLE_NAME} \
    --names    ${NAMES_FILE}    --taste ${TASTE_FILE}  > ${SCHEMA_FILE}
```

 * `DATABASE_NAME` name of the database.
 * `TABLE_NAME`    name of the table.
 * `NAMES_FILE`    names file produced in step 1 above. 
 * `TASTE_FILE`    taste file produced in step 2 above.
 * `SCHEMA_FILE`   output schema file.

Given the `accounts.names` and `account.taste` files above, we get the
following schema:

```
account_name              | string | White + AlphaNum
                          | White:9, AlphaNum:99991;

open_date                 | string | -
                          | White:1023, Day.DDcMMcYYYY('/'):98977;

close_date                | string | Day.DDcMMcYYYY('/')
                          | Day.DDcMMcYYYY('/'):100000;

active                    | string | Exact("N") + Exact("Y")
                          | Exact("N"):19877, Exact("Y"):80123;

process_id                | string | Nat
                          | Nat:100000;
```

The schema file contains one line for each column in the input table. Each
line contains a pipe separated list of fields: the column name and hive
storage types from the `.names` file, the inferred type, and a squashed
histogram from the `.taste` file.

