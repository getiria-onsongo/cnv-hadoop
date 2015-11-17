#!/usr/bin/env bash

SampleName=$1
forwardFile=$2
reverseFile=$3
numberReads=$4
hdfsPath=$5

numLines=$(($numberReads * 4))

forwardReads="${SampleName}_forwards_reads.txt"
reverseReads="${SampleName}_reverse_reads.txt"

# Split forward reads
fileNumber=0
count=1
local_file="${SampleName}_${fileNumber}_R1.fastq"
output_file="${hdfsPath}/${SampleName}_${fileNumber}_R1.fastq"
echo "${SampleName}_${fileNumber}\t${SampleName}_${fileNumber}_R1.fastq">> $forwardReads

while IFS= read -r line; do
    if [ $(($count%$numLines)) = 0 ]; then
        echo $line >> $local_file
        hadoop fs -put $local_file $output_file

        fileNumber=$((fileNumber + 1))
        local_file="${SampleName}_${fileNumber}_R1.fastq"
        output_file="${hdfsPath}/${SampleName}_${fileNumber}_R1.fastq"
        echo "${SampleName}_${fileNumber}\t${SampleName}_${fileNumber}_R1.fastq" >> $forwardReads
    else
        echo $line >> $local_file
    fi
    count=$((count + 1))
done < "${forwardFile}"

hadoop fs -put $local_file $output_file

# Split reverse reads

fileNumber=0
count=1
local_file="${SampleName}_${fileNumber}_R2.fastq"
output_file="${hdfsPath}/${SampleName}_${fileNumber}_R2.fastq"
echo "${SampleName}_${fileNumber}_R2.fastq">> $reverseReads

while IFS= read -r line; do
    if [ $(($count%$numLines)) = 0 ]; then
        echo $line >> $local_file
        hadoop fs -put $local_file $output_file

        fileNumber=$((fileNumber + 1))
        local_file="${SampleName}_${fileNumber}_R2.fastq"
        output_file="${hdfsPath}/${SampleName}_${fileNumber}_R2.fastq"
        echo "${SampleName}_${fileNumber}_R2.fastq" >> $reverseReads
    else
        echo $line >> $local_file
    fi
    count=$((count + 1))
done < "${reverseFile}"
hadoop fs -put $local_file $output_file
fileList="${SampleName}_files.txt"
output_fileList="${hdfsPath}/${SampleName}_files.txt"

paste $forwardReads $reverseReads > $fileList
hadoop fs -put $fileList $output_fileList

# Clean up
rm -rf $forwardReads
rm -rf $reverseReads
rm -rf $fileList

deleteFiles="${SampleName}_*_R*.fastq"
rm -rf $deleteFiles

