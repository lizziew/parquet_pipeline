# parquet_pipeline

If you're stuck at any time, run ```./pipeline.sh help``` to get the help menu:

```
CompressionPipeline
Usage: ./pipeline.sh [COMMAND] [ARGS]
Commands:
help
        prints help menu
gen SF
        generates tpch data with a scale factor of SF
ctop CSV SCHEMA COMPRESSION
        converts CSV with SCHEMA to parquet with COMPRESSION (none, gzip,or snappy)
        prints file sizes before and after compression, and compression time
ctoa CSV SCHEMA
        converts CSV with SCHEMA to arrow with COMPRESSION (lz4, brotli, gzip, snappy, or zstd)
        prints compression time
clean
        removes the checkpoints directory and parquet file
```

You can also refer to this README for examples. 

## Set up

**Install Spark**. For example, on Ubuntu, download the latest release from http://spark.apache.org/downloads.html and unpack with ```tar -xvf```

**Install pyarrow**. Run ```pip install pyarrow```. 

**Clone the tpch-dbgen repo.** You can use the command ```git clone https://github.com/electrum/tpch-dbgen```. 

**Create a locations.txt file in the parquet_pipeline directory.**  This will list out the files/directories you want to read and write from. Specifically, include on 3 separate lines:
- tpch-dbgen directory
- Spark directory 
- Checkpoints directory
- Directory to write Parquet files to

For example, 

```
/home/ewei/../../big_fast_drive/ewei/tpch-dbgen
/home/ewei/../../big_fast_drive/ewei/spark-2.3.2-bin-hadoop2.7/bin/spark-submit
/home/ewei/../../big_fast_drive/ewei/parquet_pipeline/checkpoints
/home/ewei/../../big_fast_drive/ewei/parquet_pipeline/lineitem-parquet
```

##  Generate data 

To generate data for the TPC-H benchmark, run ```./pipeline.sh gen SF```, where SF is the scale factor. For example, to generate with a scale factor of 10, run ```./pipeline.sh gen 10```. 

## Convert CSV to Parquet

Create a text file which describes the schema of the CSV file. Each line should describe 1 attribute, and be structured like 'column_name, column_type'. For example, for the lineitem table (TPC-H), we have:

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

Then, run ```./pipeline.sh ctop CSV SCHEMA COMPRESSION```, where CSV is the CSV file to convert to Parquet, and SCHEMA is the text file from above. COMPRESSION can be one of ```none```, ```gzip```,or ```snappy```. For example, we can run ```./pipeline.sh ctop ../tpch-dbgen/lineitem.csv lineitem_schema.txt gzip```. This command will also print out the file size before compression with gzip, and the total size of the Parquet files after compression. It also prints out the time compression takes in seconds.

## Convert CSV to Arrow (Spark DataFrame to Pandas) 

Create a text file which describes the schema of the CSV file. See the previous section, ***Convert CSV to Parquet***, for details. 

Then, run ```./pipline.sh ctoa CSV SCHEMA COMPRESSION```, where CSV is the CSV file to convert to Arrow, and SCHEMA is the text file from above. COMPRESSION can be one of ```lz4```, ```brotli```, ```gzip```, ```snappy```, or ```zstd```. For example, we can run ``` ./pipeline.sh ctoa ../tpch-dbgen/lineitem.csv lineitem_schema.txt snappy```. This command prints out the time compression takes in seconds. 

## Clean up
To remove the checkpoints directory and generated Parquet files, run ```./pipeline.sh clean```. 

## WIP: Query Parquet 

**Create a text file for the query.** For example, a TPC-H query on lineitem would be
```
SELECT l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order FROM lineitem WHERE l_shipdate <= date '1998-12-01' - interval '90' day GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus
```

**Run the query on the Parquet files.** Run ```query_parquet.py``` using the command ```./spark-2.3.2-bin-hadoop2.7/bin/spark-submit parquet_pipeline/query_parquet.py parquet_pipeline/query.txt parquet_pipeline/locations.txt```. (In other words, ```[spark command] [query_parquet.py] [query.txt] [file_locations.txt]```.) 
