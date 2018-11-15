from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import time
import sys
import os

if __name__ == "__main__":
    # Read in query
    query_file_name = sys.argv[1] 
    query_file = open(query_file_name, "r")
    query = query_file.read()
    query_name = query_file_name[:-4]

    name = sys.argv[3] 
    app_name = "BenchmarkParquet_" + query_name + "_" + name
    sc = SparkContext(appName=app_name)
    sqlContext = SQLContext(sc)

    # Get locations file 
    location_file_name = sys.argv[2]
    location_file = open(location_file_name, "r")
    locations = location_file.read().splitlines()

    # Read in Parquet files for each relation
    # relations = ["customer", "lineitem", "nation", "orders", "part", "region", "supplier"]
    relations = ["lineitem"]
    for relation in relations:
      df = sqlContext.read.parquet(locations[3] + relation + "/*.parquet")
      df.createOrReplaceTempView(relation)

    # Run query and time 
    df = sqlContext.sql(query)
