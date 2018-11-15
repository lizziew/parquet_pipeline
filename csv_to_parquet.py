import sys
import os 
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from datetime import datetime
import time

def parse_dt(s):
    return datetime.strptime(s, '%Y-%m-%d')

def parse(column_types, line):
  result = []

  items = line.split("|")
  for i in range(len(items)):
    if column_types[i] == "integer":
      result.append(int(items[i]))
    elif column_types[i] == "date":
      result.append(parse_dt(items[i]))
    elif column_types[i] == "string":
      result.append(items[i])
    elif column_types[i] == "double":
      result.append(float(items[i]))
  
  return tuple(result) 

if __name__ == "__main__":
  csv_file = sys.argv[1]
  schema_file_name = sys.argv[2]
  location_file_name = sys.argv[3] 
  compression_scheme = sys.argv[4]
  dict_encoding = sys.argv[5]
  name = sys.argv[6]  
	
  print("...WRITING TO PARQUET FOR " + name + " WITH COMPRESSION " + compression_scheme + " AND DICT ENCODING " + dict_encoding)
  
  app_name = "CSV2Parquet_" + name + "_" + compression_scheme + "_" + dict_encoding 
  sc = SparkContext(appName=app_name)
  sqlContext = SQLContext(sc)
  sqlContext.setConf("spark.sql.parquet.compression.codec", compression_scheme)
  sqlContext.setConf("parquet.enable.dictionary", dict_encoding) 

  # Read in schema 
  schema_file = open(schema_file_name, "r") 
  schema_attributes = schema_file.read().splitlines() 
  column_names = []
  column_types = []
  for attribute in schema_attributes: 
      column_names.append(attribute.split(",")[0])
      column_types.append(attribute.split(",")[1].lower().replace(" ", ""))

  structfields = []
  for i in range(len(column_names)):
    if column_types[i] == "integer":
      structfields.append(StructField(column_names[i], IntegerType(), True))
    elif column_types[i] == "double":
      structfields.append(StructField(column_names[i], DoubleType(), True))
    elif column_types[i] == "string":
     structfields.append(StructField(column_names[i], StringType(), True))
    elif column_types[i] == "date": 
      structfields.append(StructField(column_names[i], DateType(), True))
    else:
      raise ValueError("column type undefined: " + column_types[i]) 

  schema = StructType(structfields)

  print("...SCHEMA OF " + name + " IS " + str(schema))

  # Read in files
  location_file = open(location_file_name, "r")
  locations = location_file.read().splitlines() 

  sc.setCheckpointDir(locations[2] + "/" + name)
  rdd = sc.textFile(csv_file, use_unicode=False)
  rdd.checkpoint()
  rdd2 = rdd.map(lambda line: parse(column_types, line))
  df = sqlContext.createDataFrame(rdd2, schema)

  # Convert to Parquet 
  start = time.time()
  df.write.parquet(locations[3] + name) 
  end = time.time()

  # Record time and before/after file sizes  
  output_file_name = './output_' + compression_scheme + '_' + dict_encoding + '/' + name + '_' + compression_scheme + '_' + dict_encoding + '.txt'
  if os.path.exists(output_file_name):
    append_write = 'a' # append if already exists
  else:
    append_write = 'w' # make a new file if not

  output_file = open(output_file_name, append_write)

  output_file.write(str(end-start) + " seconds\n") 
  output_file.write("Before file size in bytes: " + str(os.path.getsize(csv_file)) + "\n")

  total_parquet_size = 0.0
  for f in os.listdir(locations[3] + name):
    if f.endswith(".parquet"):
      total_parquet_size += os.path.getsize(locations[3] + name + "/" + f)
  output_file.write("After file size in bytes: " + str(total_parquet_size) + "\n")

  output_file.close()
