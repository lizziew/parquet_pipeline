#./spark/bin/spark-submit csv_to_parquet.py

import sys 
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from datetime import datetime

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
  sc = SparkContext(appName="CSV2Parquet")
  sqlContext = SQLContext(sc)

  # Read in schema 
  schema_file_name = sys.argv[1]
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
      raise ValueError("column type undefined") 

  schema = StructType(structfields)

  ## Read in files
  location_file_name = sys.argv[2] 
  location_file = open(location_file_name, "r")
  locations = location_file.read().splitlines() 

  sc.setCheckpointDir(locations[0])
  rdd = sc.textFile(locations[1], use_unicode=False)
  rdd.checkpoint()
  rdd2 = rdd.map(lambda line: parse(column_types, line))
  df = sqlContext.createDataFrame(rdd2, schema)
  df.write.parquet(locations[2]) 
