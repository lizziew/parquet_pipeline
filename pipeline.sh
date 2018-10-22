#!/usr/bin/env bash

lines=(`cat "locations.txt"`)  

if [ $1 = "help" ]; then
  printf "CompressionPipeline\nUsage: ./pipeline.sh [COMMAND] [ARGS]\nCommands:\nhelp\n\tprints help menu\ngen SF\n\tgenerates tpch data with a scale factor of SF\nctop CSV SCHEMA COMPRESSION\n\tconverts CSV with SCHEMA to parquet with COMPRESSION (none, gzip,or snappy)\n\tprints file sizes before and after compression, and compression time\nctoa CSV SCHEMA COMPRESSION\n\tconverts CSV with SCHEMA to arrow with COMPRESSION (lz4, brotli, gzip, snappy, or zstd)\n\tprints compression time\nclean\n\tremoves the checkpoints directory and parquet files\n"
elif [ $1 = "gen" ]; then
  (cd ${lines[0]}; make; ./dbgen -s $2; sed 's/.$//' lineitem.tbl > lineitem.csv)
elif [ $1 = "ctop" ]; then
  ${lines[1]} --master local[16] csv_to_parquet.py $2 $3 locations.txt $4
  printf "COMPRESSION TIME\n"
  cat temp.txt; rm temp.txt 
  printf "\nBEFORE COMPRESSION\n" 
  (cd ${lines[0]}; ls -lh $2) 
  printf "\nAFTER COMPRESSION\n"
  (cd ${lines[3]}; du -sh)
  printf "\n"
elif [ $1 = "ctoa" ]; then
  ${lines[1]} --master local[16] --driver-memory 10g csv_to_arrow.py $2 $3 locations.txt $4
  printf "COMPRESSION TIME\n"
  cat temp.txt; rm temp.txt
  printf "\nBEFORE COMPRESSION\n" 
  (cd ${lines[0]}; ls -lh $2) 
  printf "\nAFTER COMPRESSION\n"
  cat tempa.txt; rm tempa.txt 
  printf "\n"
elif [ $1 == "clean" ]; then
  rm -rf ${lines[2]}; rm -rf ${lines[3]}
else
  printf "Command not valid. If stuck, please use the help flag.\n"
fi 
