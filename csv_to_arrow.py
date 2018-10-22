import sys 
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark import SparkConf
from pyspark.sql.types import *
from datetime import datetime
import pyarrow as pa
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
  conf = SparkConf().set("spark.driver.maxResultSize", "10g")
  sc = SparkContext(appName="CSV2Arrow", conf=conf)
  sqlContext = SQLContext(sc)
  sqlContext.setConf("spark.sql.execution.arrow.enabled", "true")

  # Read in schema 
  schema_file_name = sys.argv[2]
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

  # Read in files
  location_file_name = sys.argv[3] 
  location_file = open(location_file_name, "r")
  locations = location_file.read().splitlines() 

  sc.setCheckpointDir(locations[2])
  rdd = sc.textFile(sys.argv[1], use_unicode=False)
  rdd.checkpoint()
  rdd2 = rdd.map(lambda line: parse(column_types, line))
  df = sqlContext.createDataFrame(rdd2, schema)

  # Convert from Spark DataFrame to Pandas
  start = time.time()
  pdf = df.toPandas()
  end = time.time()

  # Convert from Pandas to Arrow
  table = pa.Table.from_pandas(pdf, preserve_index=False)
  batches = table.to_batches() 
  buffers = []
  for batch in batches:
      buffers.append(pa.compress(batch.serialize(), codec=sys.argv[4]))
  buffers_size = 0.0
  for buffer in buffers:
      buffers_size += buffer.size

  # Record time 
  f = open('./temp.txt', 'w')
  f.write(str(end-start) + " seconds") 
  f.close()

  # Record size
  f2 = open('./tempa.txt', 'w')
  f2.write(str(buffers_size) + " bytes")  
  f2.close() 
