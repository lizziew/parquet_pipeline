#!/usr/bin/env bash

for i in none snappy gzip
do
  for j in true false
  do
    ./benchmark.sh ctop $i $j
    for k in q1 q13 q14 q19 q3 q4 q5 q6
    do
      ./pipeline.sh queryp $k.txt "$i_$j" &> output_$i"_"$j/$k"_"$i"_"$j
    done
  done
done
