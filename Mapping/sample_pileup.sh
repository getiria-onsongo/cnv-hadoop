#!/bin/bash

s_S1_R1=/home/thyagara/data_release/umgc/hiseq/150724_700506R_0491_BHVTW2ADXX/Project_15-05617_Jul_17_2015_1/15-05617_Jul_17_2015_1_TAGGCATG_L001_R1_001.fastq
s_S1_R2=/home/thyagara/data_release/umgc/hiseq/150724_700506R_0491_BHVTW2ADXX/Project_15-05617_Jul_17_2015_1/15-05617_Jul_17_2015_1_TAGGCATG_L001_R2_001.fastq 
s_S2_R1=/home/thyagara/data_release/umgc/hiseq/150724_700506R_0491_BHVTW2ADXX/Project_15-05617_Jul_17_2015_1/15-05617_Jul_17_2015_1_TAGGCATG_L002_R1_001.fastq
s_S2_R2=/home/thyagara/data_release/umgc/hiseq/150724_700506R_0491_BHVTW2ADXX/Project_15-05617_Jul_17_2015_1/15-05617_Jul_17_2015_1_TAGGCATG_L002_R2_001.fastq

# Check to see if fastq files are compressed. If they are
# uncompress them into the working directory
#
# NOTE: The copying in the ELSE clause is not necessary. The files should be readable from data release. However, 
# there are instances where files permission are not set properly and user is unable to read files from data release. 
# This copying is a precautionary measure to make sure the program does not break if that happens. 

if [[ $s_S1_R1 = *.gz ]] ; then
    gunzip -c $s_S1_R1 > s_S1_R1.fastq
    s_S1_R1=s_S1_R1.fastq
else
    cp $s_S1_R1 s_S1_R1.fastq
    s_S1_R1=s_S1_R1.fastq
fi	

if [[ $s_S1_R2 = *.gz ]] ; then
    gunzip -c $s_S1_R2 > s_S1_R2.fastq
    s_S1_R2=s_S1_R2.fastq
else 
    cp $s_S1_R2 s_S1_R2.fastq
    s_S1_R2=s_S1_R2.fastq
fi

if [[ $s_S2_R1 = *.gz ]] ; then
    gunzip -c $s_S2_R1 > s_S2_R1.fastq
    s_S2_R1=s_S2_R1.fastq
else
    cp $s_S2_R1 s_S2_R1.fastq
    s_S2_R1=s_S2_R1.fastq
fi	

if [[ $s_S2_R2 = *.gz ]] ; then
    gunzip -c $s_S2_R2 > s_S2_R2.fastq
    s_S2_R2=s_S2_R2.fastq
else
    cp $s_S2_R2 s_S2_R2.fastq
    s_S2_R2=s_S2_R2.fastq
fi   

BWA_DB=/panfs/roc/rissdb/genomes/Homo_sapiens/hg19_canonical/bwa/hg19_canonical.fa
BOWTIE2_DB=/panfs/roc/rissdb/genomes/Homo_sapiens/hg19_canonical/bowtie2/hg19_canonical
S_DB=/panfs/roc/rissdb/genomes/Homo_sapiens/hg19_canonical/seq/hg19_canonical.fa

bwa mem -M -t 24 $BWA_DB $s_S1_R1 $s_S1_R2 > s_bwa_s1.sam
bwa mem -M -t 24 $BWA_DB $s_S2_R1 $s_S2_R2 > s_bwa_s2.sam
bowtie2 -p 24 -k 5 -x $BOWTIE2_DB -1 $s_S1_R1 -2 $s_S1_R2 -S s_bowtie2_s1.sam
bowtie2 -p 24 -k 5 -x $BOWTIE2_DB -1 $s_S2_R1 -2 $s_S2_R2 -S s_bowtie2_s2.sam

samtools view -q 10 -bS s_bwa_s1.sam > s_bwa_s1.bam
samtools view -q 10 -bS s_bwa_s2.sam > s_bwa_s2.bam
samtools view -q 10 -bS s_bowtie2_s1.sam > s_bowtie2_s1.bam
samtools view -q 10 -bS s_bowtie2_s2.sam > s_bowtie2_s2.bam

samtools merge s_bwa.bam s_bwa_s1.bam s_bwa_s2.bam
samtools merge s_bowtie2.bam s_bowtie2_s1.bam s_bowtie2_s2.bam

java -Xmx4g -jar  $CLASSPATH/picard.jar FixMateInformation SORT_ORDER=coordinate INPUT=s_bwa.bam OUTPUT=s_bwa.fixed.bam
java -Xmx4g -jar  $CLASSPATH/picard.jar MarkDuplicates REMOVE_DUPLICATES=true ASSUME_SORTED=true METRICS_FILE=s_bwa_duplicate_stats.txt INPUT=s_bwa.fixed.bam OUTPUT=s_bwa.fixed_nodup.bam
java -Xmx4g -jar  $CLASSPATH/picard.jar FixMateInformation SORT_ORDER=coordinate INPUT=s_bowtie2.bam OUTPUT=s_bowtie2.fixed.bam

samtools mpileup -f $S_DB -d 10000 -q 1 s_bwa.fixed.bam | cut -f 1,2,4 > cnv_15_05617_bwa_pileup.txt
samtools mpileup -f $S_DB -d 10000 -q 1 s_bwa.fixed_nodup.bam | cut -f 1,2,4 > cnv_15_05617_bwa_pileup_no_dup.txt
samtools mpileup -f $S_DB -d 10000 -q 1 s_bowtie2.fixed.bam | cut -f 1,2,4 > cnv_15_05617_bowtie2_pileup.txt

rm -rf *.bam
rm -rf *.sam

