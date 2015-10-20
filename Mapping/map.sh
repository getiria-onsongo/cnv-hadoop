#!/usr/bin/env bash

read offset fileLine

SampleName=$(echo "$fileLine" | cut -f1)
s_S1_R1=$(echo "$fileLine" | cut -f2)
s_S1_R2=$(echo "$fileLine" | cut -f3)

BWA_DB=/Users/onson001/Desktop/hadoop/genomes/bwa/hg19_canonical.fa
S_DB=/Users/onson001/Desktop/hadoop/genomes/seq/hg19_canonical.fa
FASTQ_PATH=/Users/onson001/Desktop/hadoop/fastq
RESULTS_PATH=/Users/onson001/Desktop/hadoop/Mapping/aligned

bwa mem -M -t 1 $BWA_DB $FASTQ_PATH/$s_S1_R1 $FASTQ_PATH/$s_S1_R2 > s_bwa.sam 

samtools view -q 10 -bS s_bwa.sam > s_bwa.bam

samtools sort s_bwa.bam s_bwa_sorted

samtools index s_bwa_sorted.bam

for chr in chrM chr1 chr2 chr3 chr4 chr5 chr6 chr7 chr8 chr9 chr10 chr11 chr12 chr13 chr14 chr15 chr16 chr17 chr18 chr19 chr20 chr21 chr22 chrX chrY
do
  samtools view -b -o $SampleName.bam s_bwa_sorted.bam $chr
  output_file=$RESULTS_PATH/$chr/$SampleName.bam
  hadoop fs -put $SampleName.bam $output_file
done
