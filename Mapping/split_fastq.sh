#!/usr/bin/env bash

# read offset fileLine

# SampleName=$(echo "$fileLine" | cut -f1)
# s_S1_R1=$(echo "$fileLine" | cut -f2)
# s_S1_R2=$(echo "$fileLine" | cut -f3)

file=/Users/onson001/Desktop/hadoop/Mapping/test.txt
RESULTS_PATH=/Users/onson001/Desktop/hadoop/Mapping/fastq
filePath=/Users/onson001/Desktop/hadoop/Mapping/files

numLines=3
SampleName="test"

forward_reads=forwards_reads.txt
reverse_reads=reverse_reads.txt

forward_file=/Users/onson001/Desktop/hadoop/Mapping/test_R1.txt
reverse_file=/Users/onson001/Desktop/hadoop/Mapping/test_R2.txt

# Split forward reads
fileNumber=0
count=1
local_file="${SampleName}_${fileNumber}_R1.fastq"
output_file="$RESULTS_PATH/${SampleName}_${fileNumber}_R1.fastq"
echo "${SampleName}\t${SampleName}_${fileNumber}_R1.fastq">> $forward_reads

while IFS= read -r line; do
    if [ $(($count%$numLines)) = 0 ]; then
	echo $line >> $local_file
	hadoop fs -put $local_file $output_file
	
	fileNumber=$((fileNumber + 1))
	local_file="${SampleName}_${fileNumber}_R1.fastq"
        output_file="$RESULTS_PATH/${SampleName}_${fileNumber}_R1.fastq"
	echo "${SampleName}\t${SampleName}_${fileNumber}_R1.fastq" >> $forward_reads
    else
	echo $line >> $local_file
    fi
    count=$((count + 1))
done < "$forward_file"
hadoop fs -put $local_file $output_file

# Split reverse reads

fileNumber=0
count=1
local_file="${SampleName}_${fileNumber}_R2.fastq"
output_file="$RESULTS_PATH/${SampleName}_${fileNumber}_R2.fastq"
echo "${SampleName}_${fileNumber}_R2.fastq">> $reverse_reads

while IFS= read -r line; do
    if [ $(($count%$numLines)) = 0 ]; then
        echo $line >> $local_file
	hadoop fs -put $local_file $output_file

        fileNumber=$((fileNumber + 1))
        local_file="${SampleName}_${fileNumber}_R2.fastq"
        output_file="$RESULTS_PATH/${SampleName}_${fileNumber}_R2.fastq"
        echo "${SampleName}_${fileNumber}_R2.fastq" >> $reverse_reads
    else
        echo $line >> $local_file
    fi
    count=$((count + 1))
done < "$reverse_file"
hadoop fs -put $local_file $output_file
fileList="${SampleName}_files.txt"
output_fileList="${filePath}/${SampleName}_files.txt"

paste $forward_reads $reverse_reads > $fileList
hadoop fs -put $fileList $output_fileList