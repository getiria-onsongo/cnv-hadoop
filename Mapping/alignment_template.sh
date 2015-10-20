#!/bin/bash
...
export BWA=<bwa-install-dir>/bwa
export SAMTOOLS=<samtools-install-dir>/samtools
export BCFTOOLS=<bcftools-install-dir>/bcftools
export VCFUTILS=<bcftools-install-dir>/vcfutils.pl
export HADOOP_HOME=<hadoop-install-dir>
export HADOOP_CONF_DIR=<hadoop-install-dir>/conf
...
# data directories
export TMP_HOME=<root-tmp-dir>/tmp
export BWA_INDEXES=<root-index-dir>/ref/bwa
...
# define ref. genome
export REF=<root-reference-dir>/hg19.fasta
### step 1: alignment”
# the KEY uniquely identifies the input file
KEY={key}
# input_file
export INPUT_FILE=${input_file}
export ANALYSIS_ID=${analysis_id}
NUM_THREAD=3
cd $TMP_HOME

$BWA aln -t $NUM_THREAD $REF $INPUT_FILE_1 > out1.sai
$BWA aln -t $NUM_THREAD $REF $INPUT_FILE_2 > out2.sai
$BWA sampe -r $INFO_RG $REF out1.sai out2.sai $INPUT_FILE_1 $INPUT_FILE_2 | \
$SAMTOOLS view -Su -F 4 - | $SAMTOOLS sort - aln.flt

# start indexing aln.flt.bam file
$SAMTOOLS index aln.flt.bam

# partition aligned data
for i in {1..22}
do
    CHR=chr$i
    $SAMTOOLS view -b -o $CHR.bam aln.flt.bam $CHR

output_file=/genome/dnaseq/output/$ANALYSIS_ID/$CHR/$KEY.$CHR.bam
$HADOOP_HOME/bin/hadoop fs -put $CHR.bam $output_file
done

# do the same thing for X, Y and M chromosomes
$SAMTOOLS view -b -o chr23.bam aln.flt.bam chrX chrY chrM
output_file=/genome/dnaseq/output/$ANALYSIS_ID/chr23/$KEY.chr23.bam
$HADOOP_HOME/bin/hadoop fs -put chr23.bam $output_file
exit 0
