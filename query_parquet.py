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
    df = sqlContext.read.parquet(locations[3] + "/*.parquet")
    df.registerTempTable("lineitem")

    # Read in query
    query_file_name = sys.argv[1] 
    query_file = open(query_file_name, "r")
    query = query_file.read()

    # Read in number of times to run query 
    total_time = 0.0
    num_iterations = int(sys.argv[3]) 

    # Warm up 3 times
    for _ in range(3):
        df = sqlContext.sql(query)

    # Run query and time 
    for _ in range(num_iterations):
        start = time.time()
        df = sqlContext.sql(query)
        end = time.time()
        total_time += (end-start)

    avg_time = float(total_time) / float(num_iterations)
    
    # Record time 
    f = open('./temp.txt', 'w')
    f.write(str(end-start) + " seconds after running " + str(num_iterations) + " iterations")
    f.close() 
