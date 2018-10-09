# parquet_pipeline

## How to use

### Install Spark

For example, on Ubuntu, download the latest release from http://spark.apache.org/downloads.html and unpack with ```tar -xvf```

###  Generate data 

**Generate a CSV file.** For example, to generate data for the TPC-H benchmark, you can: 
- Clone the repo ```git clone https://github.com/electrum/tpch-dbgen```
- Run ```cd tpch-dbgen/ && make``` 
- Generate the data with a scale factor of 10 by running ```./dbgen -s 10``` 
- In the \*.tbl files, delete the last character (the 'l' delimiter) from each line by running ```sed 's/.$//' lineitem.tbl > lineitem.csv```

### Convert CSV to Parquet

**Create a text file which describes the schema of the CSV file.** Each line should describe 1 attribute, and be structured like 'column_name, column_type'. For example, for the lineitem table (TPC-H), we have:

```
l_orderkey, Integer
l_partkey, Integer
l_suppkey, Integer
l_linenumber, Integer
l_extendedprice, Double
l_discount, Double
l_tax, Double
l_returnflag, String
l_linestatus, String
l_shipdate, Date
l_commitdate, Date
l_receiptdate, Date
l_shipinstruct, String
l_shipmode, String
l_comment, String
```

The column_type should be Integer, Double, String, or Date, and it's case insensitive.

**Create a text file which lists out the files/directories you want to read and write from.** Specifically, include on 3 separate lines:
- Checkpoints directory
- Csv file
- Directory to write Parquet files to

For example, 

```
/home/ewei/../../big_fast_drive/ewei/parquet_pipeline/checkpoints
/home/ewei/../../big_fast_drive/ewei/tpch-dbgen/lineitem.csv
/home/ewei/../../big_fast_drive/ewei/parquet_pipeline/lineitem-parquet
```

**Run csv_to_parquet.py.** Run ```csv_to_parquet.py``` using the command ```./spark/bin/spark-submit parquet_pipeline/csv_to_parquet.py parquet_pipeline/lineitem_schema.txt locations.txt```. (In other words, ```[spark command] [csv_to_parquet.py] [schema.txt] [file_locations.txt]```, where schema.txt and file_locations.txt are the two text files you created in the previous two steps.)

### Query Parquet 

**Create a text file for the query.** For example, a TPC-H query on lineitem would be

```
SELECT l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order FROM lineitem WHERE l_shipdate <= date '1998-12-01' - interval '90' day GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus
```

**Run the query on the Parquet files.** Run ```query_parquet.py``` using the command ```./spark-2.3.2-bin-hadoop2.7/bin/spark-submit parquet_pipeline/query_parquet.py parquet_pipeline/query.txt parquet_pipeline/locations.txt```. (In other words, ```[spark command] [query_parquet.py] [query.txt] [file_locations.txt]```.) 
