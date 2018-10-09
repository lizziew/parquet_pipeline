from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import time
import sys

if __name__ == "__main__":
    sc = SparkContext(appName="BenchmarkParquet")
    sqlContext = SQLContext(sc)

    # Read in Parquet location
    location_file_name = sys.argv[2]
    location_file = open(location_file_name, "r")
    locations = location_file.read().splitlines()
    df = sqlContext.read.parquet(locations[2])
    df.registerTempTable("lineitem")

    # Read in  query
    query_file_name = sys.argv[1] 
    query_file = open(query_file_name, "r")
    query = query_file.read()

    # Run query 
    start = time.time()
    df = sqlContext.sql(query)
    df.show(n = 5)
    end = time.time()
    print(end - start)
