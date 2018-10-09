# parquet_pipeline

## How to use

### Install Spark

For example, on Ubuntu, download the latest release from http://spark.apache.org/downloads.html and unpack with ```tar -xvf```

###  Generate data 

**Generate a CSV file(s).** For example, to generate data for the TPC-H benchmark, you can: 
- Clone the repo ```git clone https://github.com/electrum/tpch-dbgen```
- Run ```cd tpch-dbgen/ && make``` 
- Generate the data with a scale factor of 10 ```./dbgen -s 10``` 
- In the \*.tbl files, delete the last character (the 'l' delimiter) from each line ```sed 's/.$//' lineitem.tbl > lineitem.csv```

### Convert CSV to Parquet

Setup will be different from person to person, but the command to run ```csv_to_parquet.py``` will be similar to ```./spark/bin/spark-submit parquet_pipeline/csv_to_parquet.py```
