#!/usr/bin/env bash

lines=(`cat "locations.txt"`)  

if [ $1 = "help" ]; then
  printf "CompressionPipeline\nUsage: ./pipeline.sh [COMMAND] [ARGS]\nCommands:\nhelp\n\tprints help menu\ngent SF\n\tgenerates tpch data with a scale factor of SF\ngeng SCHEMA SIZE\n\tgenerates a CSV file (gendata.csv) with random values following SCHEMA and size SIZE (in GB)\nctop CSV SCHEMA COMPRESSION DICT NAME\n\tconverts CSV with SCHEMA representing NAME relation to parquet\n\toptions: COMPRESSION (none, gzip, snappy) and DICT (true, false)\n\tprints file sizes before and after compression, and compression time\nctoa CSV SCHEMA COMPRESSION\n\tconverts CSV with SCHEMA to arrow with COMPRESSION (lz4, brotli, gzip, snappy, or zstd)\n\tprints compression time\nqueryp QUERY NAME\n\truns query in QUERY on Parquet files\n\tshould be run after the ctop command\n\tsaves time to execute query in NAME directory\nclean\n\tremoves the checkpoints directory and parquet files\npeek ARGS\n\tcalls parquet-tools with ARGS\n\tARGS='schema PARQUET_FILE': get schema\n\tARGS='cat PARQUET_FILE': get data\n\tARGS='meta PARQUET_FILE': get metadata\n\tARGS='--help': show parquet-tools help menu\n\tARGS='dump PARQUET_FILE': more verbose version of meta\n"
elif [ $1 = "gent" ]; then
  (cd ${lines[0]}; make; ./dbgen -s $2; sed 's/.$//' lineitem.tbl > lineitem.csv; sed 's/.$//' customer.tbl > customer.csv; sed 's/.$//' orders.tbl > orders.csv; sed 's/.$//' supplier.tbl > supplier.csv; sed 's/.$//' nation.tbl > nation.csv; sed 's/.$//' region.tbl > region.csv; sed 's/.$//' part.tbl > part.csv;)
elif [ $1 = "geng" ]; then
  python gen_data.py $2 $3 
elif [ $1 = "ctop" ]; then
  ${lines[1]} --master local[16] --driver-memory 30g csv_to_parquet.py $2 $3 locations.txt $4 $5 $6
elif [ $1 = "ctoa" ]; then
  ${lines[1]} --master local[16] --driver-memory 30g csv_to_arrow.py $2 $3 locations.txt $4
  printf "COMPRESSION TIME\n"
  cat temp.txt; rm temp.txt
  printf "\nBEFORE COMPRESSION\n" 
  (cd ${lines[0]}; ls -lh $2) 
  printf "\nAFTER COMPRESSION\n"
  cat tempa.txt; rm tempa.txt 
  printf "\n"
elif [ $1 = "peek" ]; then
  java -jar ${lines[4]} "${@:2}"
elif [ $1 == "queryp" ]; then
  ${lines[1]} --master local[16] --driver-memory 30g --conf spark.eventLog.enabled=true query_parquet.py $2 locations.txt $3
elif [ $1 == "clean" ]; then
  rm -rf ${lines[2]}; rm -rf ${lines[3]}
else
  printf "Command not valid. If stuck, please use the help flag.\n"
fi 
