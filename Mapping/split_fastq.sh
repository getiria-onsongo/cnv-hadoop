#!/usr/bin/env bash

read offset fileLine

SampleName=$(echo "$fileLine" | cut -f1)
forward_file=$(echo "$fileLine" | cut -f2)
reverse_file=$(echo "$fileLine" | cut -f3)

numLines=$(($number_reads * 4))

forward_reads=forwards_reads.txt
reverse_reads=reverse_reads.txt


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
