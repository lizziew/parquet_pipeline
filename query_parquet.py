from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import time
import sys
import os

if __name__ == "__main__":
    sc = SparkContext(appName="BenchmarkParquet")
    sqlContext = SQLContext(sc)
    name = sys.argv[4] 

    # Read in query
    query_file_name = sys.argv[1] 
    query_file = open(query_file_name, "r")
    query = query_file.read()
    query_name = query_file_name[:-4]

    # Make output file 
    output_file_name = './output_' + name + '/' + query_name + '.txt'
    if os.path.exists(output_file_name):
      append_write = 'a' # append if already exists
    else:
      append_write = 'w' # make a new file if not

    output_file = open(output_file_name, append_write)
 
    # Get locations file 
    location_file_name = sys.argv[2]
    location_file = open(location_file_name, "r")
    locations = location_file.read().splitlines()

    # Read in Parquet files for each relation
    relations = ["customer", "lineitem", "nation", "orders", "part", "region", "supplier"]
    for relation in relations:
      df = sqlContext.read.parquet(locations[3] + relation + "/*.parquet")
      df.createOrReplaceTempView(relation)

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
        output_file.write("     " + str(end-start) + " seconds\n")
        total_time += (end-start)

    # Record average time
    avg_time = float(total_time) / float(num_iterations)
    output_file.write(str(end-start) + " seconds after running " + str(num_iterations) + " iterations\n")
    output_file.close()
