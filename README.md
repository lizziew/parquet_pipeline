# parquet_pipeline

If you're stuck at any time, run ```./pipeline.sh help``` to get the help menu:

```
CompressionPipeline
Usage: ./pipeline.sh [COMMAND] [ARGS]
Commands:
help
        prints help menu
gent SF
        generates tpch data with a scale factor of SF
geng SCHEMA SIZE
        generates a CSV file (gendata.csv) with random values following SCHEMA and size SIZE (in GB)
ctop CSV SCHEMA COMPRESSION
        converts CSV with SCHEMA to parquet with COMPRESSION (none, gzip,or snappy)
        prints file sizes before and after compression, and compression time
ctoa CSV SCHEMA COMPRESSION
        converts CSV with SCHEMA to arrow with COMPRESSION (lz4, brotli, gzip, snappy, or zstd)
        prints compression time
queryp NUM_ITERATIONS
        runs query in query.txt on Parquet files for NUM_ITERATIONS iterations (after warming up)
        should be run after the ctop command
        prints time to execute query
clean
        removes the checkpoints directory and parquet files
peek ARGS
        calls parquet-tools with ARGS
        ARGS='schema PARQUET_FILE': get schema
        ARGS='cat PARQUET_FILE': get data
        ARGS='meta PARQUET_FILE': get metadata
        ARGS='--help': show parquet-tools help menu
        ARGS='dump PARQUET_FILE': more verbose version of meta
```

You can also refer to this README for examples. 

## Set up

**Install Spark**. For example, on Ubuntu, download the latest release from http://spark.apache.org/downloads.html and unpack with ```tar -xvf```

**Install pyarrow**. Run ```pip install pyarrow```. 

**Install parquet-tools**. Download the latest release from https://github.com/apache/parquet-mr/releases and unpack with ```tar xvzf```. Then ```cd parquet-tools``` and build with ```mvn clean package -Plocal```. 

**Clone the tpch-dbgen repo.** You can use the command ```git clone https://github.com/electrum/tpch-dbgen```. 

**Create a locations.txt file in the parquet_pipeline directory.**  This will list out the files/directories you want to read and write from. Specifically, include on 4 separate lines:
- Directory the CSV file is in (e.g. tpch-dbgen directory) 
- Spark directory 
- Checkpoints directory
- Directory to write Parquet files to
- Location of parquet-tools jar 

For example, 

```
/home/tpch-dbgen
/home/spark-2.4.0-bin-hadoop2.7/bin/spark-submit
/home/parquet_pipeline/checkpoints
/home/lineitem-parquet
/home/parquet-mr-apache-parquet-1.8.3/parquet-tools/target/parquet-tools-1.8.3.jar
```

##  Generate data 

To generate data for the TPC-H benchmark, run ```./pipeline.sh gent SF```, where SF is the scale factor. For example, to generate with a scale factor of 10, run ```./pipeline.sh gent 10```. 

To generate data with your own specified schema, run ```./pipeline.sh geng SCHEMA SIZE```, where SCHEMA is a text file containing your schema (see next section for how to create this file), and SIZE is the desired size in GB.  

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

## Convert CSV to Arrow 

Create a text file which describes the schema of the CSV file. See the previous section, ***Convert CSV to Parquet***, for details. 

Then, run ```./pipeline.sh ctoa CSV SCHEMA COMPRESSION```, where CSV is the CSV file to convert to Arrow, and SCHEMA is the text file from above. COMPRESSION can be one of ```lz4```, ```brotli```, ```gzip```, ```snappy```, or ```zstd```. For example, we can run ``` ./pipeline.sh ctoa ../tpch-dbgen/lineitem.csv lineitem_schema.txt snappy```. This command prints out the time compression takes in seconds. 

## Query on Parquet file

**Create a text file, query.txt, for the query.** For example, a TPC-H query on lineitem would be
```
SELECT l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order FROM lineitem WHERE l_shipdate <= date '1998-12-01' - interval '90' day GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus
```

**Run the query on the Parquet files.** Run ```./pipeline.sh queryp NUM_ITERATIONS```, where NUM_ITERATIONS is the number of times to run the query and time it. The final reported time will be the average of those times. (Each query is also run 3 times beforehand to warm up. These 3 times are not included in the final reported time.) The query is over the Parquet files specified in the last line of locations.txt. 

## Clean up
To remove the checkpoints directory and generated Parquet files, run ```./pipeline.sh clean```. 

## Use parquet-tools
Examples of commands are ```./pipeline.sh peek schema ../lineitem-parquet/part-00000-f031d4dd-09d8-40fe-b62c-207e792de2ab-c000.parquet``` or ```./pipeline.sh peek --help```
