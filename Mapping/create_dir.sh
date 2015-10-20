#!/usr/bin/env bash

RESULTS_PATH=/Users/onson001/Desktop/hadoop/Mapping/aligned

for chr in chrM chr1 chr2 chr3 chr4 chr5 chr6 chr7 chr8 chr9 chr10 chr11 chr12 chr13 chr14 chr15 chr16 chr17 chr18 chr19 chr20 chr21 chr22 chrX chrY
do
    dir_to_create=$RESULTS_PATH/$chr
    hdfs dfs -mkdir -p $dir_to_create
done

