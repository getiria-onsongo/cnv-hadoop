#!/usr/bin/env bash

# read offset fileLine

# SampleName=$(echo "$fileLine" | cut -f1)
# s_S1_R1=$(echo "$fileLine" | cut -f2)
# s_S1_R2=$(echo "$fileLine" | cut -f3)

file=/Users/onson001/Desktop/hadoop/Mapping/test.txt
RESULTS_PATH=/Users/onson001/Desktop/hadoop/Mapping/fastq

fileNumber=0
count=1
numLines=3

while IFS= read -r line; do
    if [ $(($count%$numLines)) = 0 ]; then
	echo "file number $fileNumber"
	fileNumber=$((fileNumber + 1))
    fi
    count=$((count + 1))
    echo $line
done < "$file"

echo "file number $fileNumber"