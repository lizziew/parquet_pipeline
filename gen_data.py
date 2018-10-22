import sys 
import csv
import random 
import math
import os 

if __name__ == "__main__":
  # Read in schema to generate  
  schema_file_name = sys.argv[1]
  schema_file = open(schema_file_name, "r") 
  schema_attributes = schema_file.read().splitlines() 
  column_names = []
  column_types = []
  for attribute in schema_attributes: 
    column_names.append(attribute.split(",")[0])
    column_types.append(attribute.split(",")[1].lower().replace(" ", ""))

  # Read in number of GB we want to generate
  total_size = float(sys.argv[2]) * math.pow(10, 9)  
  
  # Write CSV file 
  with open('gendata.csv', mode='w') as data_file:
    data_writer = csv.writer(data_file, delimiter='|', quoting=csv.QUOTE_MINIMAL)
    while os.path.getsize('./gendata.csv') <= total_size: 
      row = []
      for i in range(len(column_names)):
        if column_types[i] == "integer":
          row.append(random.randint(-2147483648, 2147483647))
        else:
          raise ValueError("column type undefined")
      data_writer.writerow(row)
