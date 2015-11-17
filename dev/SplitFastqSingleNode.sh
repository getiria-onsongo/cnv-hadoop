#!/usr/bin/env bash

fileName=$1
hdfsPath=$2
numberReads=$3
runName=$4

forwardReads=forwards_reads.txt
reverseReads=reverse_reads.txt

SplitPairedReads(){

SampleName=$1
forwardFile=$2
reverseFile=$3

numLines=$(($numberReads * 4))

# Split forward reads
fileNumber=0
count=1
local_file="${SampleName}_${fileNumber}_R1.fastq"
output_file="${hdfsPath}/${SampleName}_${fileNumber}_R1.fastq.gz"

while IFS= read -r line; do
    if [ $(($count%$numLines)) = 0 ]; then
        echo $line >> $local_file
        gzip $local_file
        hadoop fs -put "${local_file}.gz" $output_file
        printf "${SampleName}_${fileNumber}\t${SampleName}_${fileNumber}_R1.fastq \n">> $forwardReads
        fileNumber=$((fileNumber + 1))
        local_file="${SampleName}_${fileNumber}_R1.fastq"
        output_file="${hdfsPath}/${SampleName}_${fileNumber}_R1.fastq.gz"
    else
        echo $line >> $local_file
    fi
    count=$((count + 1))
done < "${forwardFile}"

if [ -s $local_file ]
then
    printf "${SampleName}_${fileNumber}\t${SampleName}_${fileNumber}_R1.fastq \n">> $forwardReads
    gzip $local_file
    hadoop fs -put "${local_file}.gz" $output_file
fi
# Split reverse reads

fileNumber=0
count=1
local_file="${SampleName}_${fileNumber}_R2.fastq"
output_file="${hdfsPath}/${SampleName}_${fileNumber}_R2.fastq.gz"

while IFS= read -r line; do
    if [ $(($count%$numLines)) = 0 ]; then
        echo $line >> $local_file
        gzip $local_file
        hadoop fs -put "${local_file}.gz" $output_file
        printf "${SampleName}_${fileNumber}_R2.fastq \n" >> $reverseReads
        fileNumber=$((fileNumber + 1))
        local_file="${SampleName}_${fileNumber}_R2.fastq"
        output_file="${hdfsPath}/${SampleName}_${fileNumber}_R2.fastq.gz"

    else
        echo $line >> $local_file
    fi
    count=$((count + 1))
done < "${reverseFile}"

if [ -s $local_file ]
then
    printf "${SampleName}_${fileNumber}_R2.fastq \n" >> $reverseReads
    gzip $local_file
    hadoop fs -put "${local_file}.gz" $output_file
fi

deleteFiles="${SampleName}_*_R*.fastq.gz"
rm -rf $deleteFiles

}

while IFS= read -r line; do
    a=$(echo $line | tr ',' "\n")
    SplitPairedReads ${a[0]} ${a[1]} ${a[2]}
done < $fileName

fileList="${runName}_fastq_files.txt"
output_fileList="${hdfsPath}/${runName}_fastq_files.txt"

paste $forwardReads $reverseReads > $fileList
hadoop fs -put $fileList $output_fileList

#------ Clean up
rm -rf $forwardReads
rm -rf $reverseReads
rm -rf $fileList

