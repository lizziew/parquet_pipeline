#!/usr/bin/env bash

lines=(`cat "locations.txt"`)

if [ $1 = "help" ]; then
  printf "Benchmark\nUsage: ./benchmark.sh [COMMAND] [ARGS]\nCommands:\nhelp\n\tprints help menu\nctop COMPRESSION DICT\n\tconverts all tpch csvs to parquet with COMPRESSION (none, gzip, snappy) and DICT (true, false)\nqueryp QUERY COMPRESSION DICT\n\truns tpch QUERY (specify with q1, q2,... etc)\n\tsaved in COMPRESSION_DICT directory\n"
elif [ $1 = "ctop" ]; then
  ./pipeline.sh clean
  rm -rf "output_$2_$3"
  mkdir "output_$2_$3"
  ./pipeline.sh ctop "${lines[0]}/customer.csv" customer_schema.txt $2 $3 customer 
  ./pipeline.sh ctop "${lines[0]}/lineitem.csv" lineitem_schema.txt $2 $3 lineitem 
  ./pipeline.sh ctop "${lines[0]}/nation.csv" nation_schema.txt $2 $3 nation
  ./pipeline.sh ctop "${lines[0]}/orders.csv" orders_schema.txt $2 $3 orders 
  ./pipeline.sh ctop "${lines[0]}/part.csv" part_schema.txt $2 $3 part
  ./pipeline.sh ctop "${lines[0]}/region.csv" region_schema.txt $2 $3 region
  ./pipeline.sh ctop "${lines[0]}/supplier.csv" supplier_schema.txt $2 $3 supplier
elif [ $1 == "queryp" ]; then
  ./pipeline.sh queryp q1.txt 10 "$2_$3"
  ./pipeline.sh queryp q13.txt 10 "$2_$3"
  ./pipeline.sh queryp q14.txt 10 "$2_$3"
  ./pipeline.sh queryp q19.txt 10 "$2_$3"
  ./pipeline.sh queryp q3.txt 10 "$2_$3"
  ./pipeline.sh queryp q4.txt 10 "$2_$3"
  ./pipeline.sh queryp q5.txt 10 "$2_$3"
  ./pipeline.sh queryp q6.txt 10 "$2_$3"
else
  printf "Command not valid. If stuck, please use the help flag.\n"
fi
