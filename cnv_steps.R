
hadoop fs -rm -r fastq

hadoop fs -mkdir fastq

sh SplitFastqSingleNode.sh input.txt fastq 10000 heart

hadoop com.sun.tools.javac.Main BwaMapping.java
jar cf BwaMapping.jar *.class
hadoop jar BwaMapping.jar BwaMapping heart BwaMappingOut "genomes/bwa.tar.gz" "fastq"



** 2) Look at samples that did not pass and have at least 3 windows to see what is happening.
** 3) Look at googles free machine learning package.
** 1) Run the CNV pipeline on exome data with one sample as control and the rest as samples.
**** Translocation detection
**** Reducing windows to 1

cd /Users/onson001/Desktop/hadoop/dev
hstart
cp /Users/onson001/Desktop/hadoop/Mapping/BwaMapping/dev/BwaMapping.java .

hadoop fs -rm -r genomes
hadoop fs -mkdir genomes
hadoop fs -put /Users/onson001/Desktop/hadoop/genomes/bwa.tar.gz genomes/bwa.tar.gz

hadoop fs -rm -r fastq
hadoop fs -mkdir fastq
sh SplitFastqSingleNode.sh input.txt fastq 10000

sh SplitFastqSingleNode.sh heart-1 heart-1_R1.fastq heart-1_R2.fastq 10000 fastq

hadoop com.sun.tools.javac.Main BwaMapping.java
jar cf BwaMapping.jar *.class
hadoop jar BwaMapping.jar BwaMapping "fastq/heart-1_files.txt" BwaMappingOut "genomes/bwa.tar.gz" "fastq"

hadoop com.sun.tools.javac.Main Test.java
jar cf Test.jar *.class
hadoop jar Test.jar Test "fastq/fastq_files.txt" TestOut "fastq"











# NEXT:
# 1) TEST MAPPING ON MSI
# 2) COMBINE SAM FILES INTO ONE FILE.

# Code source:
# https://github.com/mahmoudparsian/data-algorithms-book

# Fixing excel tab files

tr '\r' '\n' < test_data.txt > test_data_clean.txt
mv test_data_clean.txt test_data.txt

# Start hadoop
hstart

# Upload files to Hadoop file System

# CREATE TEST FILES SMALL ENOUGH TO TEST IMPLEMENTATION

cut -f3,4 out.txt > temp1.txt

awk -F, '{$(NF+1)=40;}1' OFS="\t" temp1.txt > control_bwa_no_dup.txt
awk -F, '{$(NF+1)=40;}1' OFS="\t" temp1.txt > control_bowtie.txt
awk -F, '{$(NF+1)=40;}1' OFS="\t" temp1.txt > control_bwa.txt

awk -F, '{$(NF+1)=20;}1' OFS="\t" temp1.txt > sample_bwa_no_dup.txt
awk -F, '{$(NF+1)=20;}1' OFS="\t" temp1.txt > sample_bowtie.txt
awk -F, '{$(NF+1)=20;}1' OFS="\t" temp1.txt > sample_bwa.txt

hadoop fs -put raw_data/control_bwa_no_dup.txt fs_data/control_bwa_no_dup.txt
hadoop fs -put raw_data/control_bowtie.txt fs_data/control_bowtie.txt
hadoop fs -put raw_data/control_bwa.txt fs_data/control_bwa.txt

hadoop fs -put raw_data/sample_bwa_no_dup.txt fs_data/sample_bwa_no_dup.txt
hadoop fs -put raw_data/sample_bowtie.txt fs_data/sample_bowtie.txt
hadoop fs -put raw_data/sample_bwa.txt fs_data/sample_bwa.txt

# Create reference pileup for 3 of the 11 references
# NOTE: Unlike in MySQL where we had to join a pileup table and the reference table (tso_reference),
# here we will generate the pileup table for references from tso_reference directly. No need to
# pre-generate the pileup table.
#
#
# SINGLE MACHINE TASK
javac *.java
java GetRandomReferences raw_data/tso_reference.txt raw_data/reference_pileup.txt 3


# JOIN REFERENCE WITH SAMPLE

pig -f Pig/join_coverage_reference.pig -param cov_input='/Users/onson001/Desktop/hadoop/fs_data/sample_bwa_no_dup.txt' \
-param ref_input='/Users/onson001/Desktop/hadoop/fs_data/reference_pileup.txt' \
-param output='/Users/onson001/Desktop/hadoop/fs_data/sample_reference_pileup'

hadoop fs -getmerge /Users/onson001/Desktop/hadoop/fs_data/sample_reference_pileup /Users/onson001/Desktop/hadoop/fs_data/sample_reference_pileup.txt

# JOIN REFERENCE WITH CONTROL
pig -f Pig/join_coverage_reference.pig -param cov_input='/Users/onson001/Desktop/hadoop/fs_data/control_bwa_no_dup.txt' \
-param ref_input='/Users/onson001/Desktop/hadoop/fs_data/reference_pileup.txt' \
-param output='/Users/onson001/Desktop/hadoop/fs_data/control_reference_pileup'

hadoop fs -getmerge /Users/onson001/Desktop/hadoop/fs_data/control_reference_pileup /Users/onson001/Desktop/hadoop/fs_data/control_reference_pileup.txt

# Find median reference coverage for sample
java FindMedian /Users/onson001/Desktop/hadoop/fs_data/sample_reference_pileup.txt /Users/onson001/Desktop/hadoop/fs_data/sample_reference_median.txt

# Find median reference coverage for control
java FindMedian /Users/onson001/Desktop/hadoop/fs_data/control_reference_pileup.txt /Users/onson001/Desktop/hadoop/fs_data/control_reference_median.txt

# Hadoop requires files in distributed cache be publicly accessible so I am copying these files to /tmp
cp /Users/onson001/Desktop/hadoop/fs_data/sample_reference_median.txt /tmp/sample_reference_median.txt
cp /Users/onson001/Desktop/hadoop/fs_data/control_reference_median.txt /tmp/control_reference_median.txt

# Compute within sample ratio
hadoop fs -rm -r fs_output/WithinRatioOutSample
hadoop jar WithinRatio/WithinRatio.jar WithinRatio fs_data/sample_bwa_no_dup.txt fs_output/WithinRatioOutSample /tmp/sample_reference_median.txt

# Compute within control ratio
hadoop fs -rm -r fs_output/WithinRatioOutControl
hadoop jar WithinRatio/WithinRatio.jar WithinRatio fs_data/control_bwa_no_dup.txt fs_output/WithinRatioOutControl /tmp/control_reference_median.txt

# COMPUTE COVERAGE RATIO

hadoop fs -rm -r fs_data/coverage_ratio

pig -f Pig/compute_coverage.pig \
-param sample_input='/Users/onson001/Desktop/hadoop/fs_output/WithinRatioOutSample' \
-param control_input='/Users/onson001/Desktop/hadoop/fs_output/WithinRatioOutControl' \
-param output='/Users/onson001/Desktop/hadoop/fs_data/coverage_ratio'

# COMPUTE BOWTIE/BWA RATIO

hadoop fs -rm -r fs_data/bb_ratio

pig -f Pig/compute_bowtie_bwa.pig \
-param sample_bowtie_input='/Users/onson001/Desktop/hadoop/fs_data/sample_bowtie.txt' \
-param sample_bwa_input='/Users/onson001/Desktop/hadoop/fs_data/sample_bwa.txt' \
-param output='/Users/onson001/Desktop/hadoop/fs_data/bb_ratio'

# ADD gene_symbol TO BOWTIE/BWA RATIO

hadoop fs -rm -r fs_data/bb_ratio_gene

pig -f Pig/combine_bb_ratio_gene_symbol.pig \
-param sample_bb_ratio_input='/Users/onson001/Desktop/hadoop/fs_data/bb_ratio' \
-param exon_pileup_input='/Users/onson001/Desktop/hadoop/fs_common_data/tso_exon_contig_pileup.txt' \
-param output='/Users/onson001/Desktop/hadoop/fs_data/bb_ratio_gene'



# ----- COMBINE DATA IN coverage_ratio AND bb_ratio_gene AND SEPARATE THE THREE REFERENCES

hadoop fs -rm -r fs_data/temp1
hadoop fs -rm -r fs_data/temp2
hadoop fs -rm -r fs_data/ref1
hadoop fs -rm -r fs_data/ref2
hadoop fs -rm -r fs_data/ref3

pig -f Pig/combine_bb_ratio_gene_coverage.pig \
-param coverage_ratio_input='/Users/onson001/Desktop/hadoop/fs_data/coverage_ratio' \
-param bb_ratio_gene_input='/Users/onson001/Desktop/hadoop/fs_data/bb_ratio_gene' \
-param temp1='/Users/onson001/Desktop/hadoop/fs_data/temp1' \
-param temp2='/Users/onson001/Desktop/hadoop/fs_data/temp2' \
-param output1='/Users/onson001/Desktop/hadoop/fs_data/ref1' \
-param output2='/Users/onson001/Desktop/hadoop/fs_data/ref2' \
-param output3='/Users/onson001/Desktop/hadoop/fs_data/ref3'

# OUTPUT FIELDS = ref_contig,gene_symbol,chr,pos,coverage_ratio,bb_ratio
# ------
# NOTE: Compile multiple java file (hadoop com.sun.tools.javac.Main *.java)
# hadoop com.sun.tools.javac.Main ComputeAverage.java
# jar cf ComputeAverage.jar *.class
# hadoop jar ComputeAverage/ComputeAverage.jar ComputeAverage fs_data/test1.txt ComputeAverageOut


# Normalize data
# a) Find average coverage across genome between 0.5 and 2.0
#           i) all_ratios = 0.5 < ratio < 2.0)
#           ii) avg_log2 = mean(log2(all_ratios))
# b) Normalize in log 2 space
#           i) normalized_value = 2^(log2(value) - avg_log2)
# NOTE: Only ratio is normalized

hadoop jar ComputeAverageInMapComb/ComputeAverageInMapComb.jar ComputeAverageInMapComb fs_data/ref1 /tmp/ref1
hadoop jar ComputeAverageInMapComb/ComputeAverageInMapComb.jar ComputeAverageInMapComb fs_data/ref2 /tmp/ref2
hadoop jar ComputeAverageInMapComb/ComputeAverageInMapComb.jar ComputeAverageInMapComb fs_data/ref3 /tmp/ref3

hadoop fs -getmerge /tmp/ref1 /tmp/ref/ref1.txt
hadoop fs -getmerge /tmp/ref2 /tmp/ref/ref2.txt
hadoop fs -getmerge /tmp/ref3 /tmp/ref/ref3.txt

hadoop jar NormalizeRatio/NormalizeRatio.jar NormalizeRatio fs_data/ref1 /tmp/ref/ref1.txt fs_data/ref1_norm
hadoop jar NormalizeRatio/NormalizeRatio.jar NormalizeRatio fs_data/ref2 /tmp/ref/ref2.txt fs_data/ref2_norm
hadoop jar NormalizeRatio/NormalizeRatio.jar NormalizeRatio fs_data/ref3 /tmp/ref/ref3.txt fs_data/ref3_norm

# START HERE

cd /Users/onson001/Desktop/hadoop/MovingAverageExample
javac *.java
java TestSimpleMovingAverage 3

hadoop com.sun.tools.javac.Main *.java
jar cf MovingAverage.jar *.class
cd ..
hadoop fs -rm -r MovingAverageOut
hadoop jar MovingAverage/MovingAverage.jar SortByMRF_MovingAverageDriver 3 fs_data/MovingAvgTest  MovingAverageOut


# NEXT: Create a class that takes in array and uses SimpleMovingAverageUsingArray.java to compute moving average
# returning an array of moving average values


# Rolling mean (window = 200)
# if(window_length >= gene_length/4)
#   window_length = round(gene_length/4)
# if(window_length < 1)
#     window_length = 1
# NOTE: smoothing is done on both ratio and bowtie/bwa
R CMD BATCH smooth_coverage.R


# Star of Mapping
hadoop com.sun.tools.javac.Main -cp ../freemarker.jar TemplateEngine.java


HADOOP_HOME=/usr/local/Cellar/hadoop/2.6.0

# Split fastq files

isub -m 8gb -w 1:00:00

cd split

module load pig

hadoop fs -rm -r fastq

hadoop jar /soft/hadoop/1.2.1/contrib/streaming/hadoop-streaming-1.2.1.jar \
-D mapred.reduce.tasks=0 \
-D mapred.map.tasks.speculative.execution=false \
-D mapred.task.timeout=86400000 \
-input fastq_files.txt \
-inputformat org.apache.hadoop.mapred.lib.NLineInputFormat \
-cmdenv number_reads=3000000 \
-cmdenv RESULTS_PATH="fastq" \
-cmdenv fastqPath="raw_fastq" \
-cmdenv filePath="files" \
-output fastq \
-mapper split_fastq.sh \
-file split_fastq.sh

cd files

cat $(find  . -name "*.txt"  -type f) > file_list.txt

# Align

hadoop fs -rm -r output

hadoop fs -rm -r aligned

sh create_dir.sh

hadoop jar $HADOOP_HOME/libexec/share/hadoop/tools/lib/hadoop-streaming-2.6.0.jar \
-D mapred.reduce.tasks=0 \
-D mapred.map.tasks.speculative.execution=false \
-D mapred.task.timeout=86400000 \
-input sample_fastq.txt \
-inputformat org.apache.hadoop.mapred.lib.NLineInputFormat \
-output output \
-mapper map.sh \
-file map.sh

hadoop jar /panfs/roc/itascasoft/hadoop/2.7.1/share/hadoop/tools/lib/hadoop-streaming-2.7.1.jar
-input /groups/riss/onson001/split/fastq_files.txt \
-inputformat org.apache.hadoop.mapred.lib.NLineInputFormat
-cmdenv number_reads=3000000 \
-cmdenv RESULTS_PATH="/groups/riss/onson001/split/fastq" \
-cmdenv fastqPath="/groups/riss/onson001/split/raw_fastq" \
-cmdenv filePath="/groups/riss/onson001/split/files" \
-output /groups/riss/onson001/split/output \
-mapper /scratch.global/onson001/split/split_fastq.sh
-file /scratch.global/onson001/split/split_fastq.sh


hadoop fs -rm -r /groups/riss/onson001/split

hadoop fs -put split /groups/riss/onson001/split
hadoop fs -put test.fastq /groups/riss/onson001/split/test.fastq
hadoop fs -rm -r /groups/riss/onson001/split/test.sh
hadoop fs -put test.sh /groups/riss/onson001/split/test.sh
hadoop fs -ls /groups/riss/onson001/split/test.fastq

hadoop fs -rm -r /groups/riss/onson001/split/fastq_files.txt
hadoop fs -put fastq_files.txt /groups/riss/onson001/split/fastq_files.txt

cd split

-file "/home/msistaff/onson001/split/11-05910_R1.fastq" \
-file "/home/msistaff/onson001/split/11-05910_R2.fastq" \
-cmdenv R1="/home/msistaff/onson001/split/11-05910_R1.fastq#R1" \
-cmdenv R2="/home/msistaff/onson001/split/11-05910_R2.fastq" \

/groups/riss/onson001/split/test.fastq,



# --------------------
# Note. The generic options (e.g., -files) must be specified before the streaming options.

ssh mesabi

module load hadoop/2.7.1

hadoop fs -rm -r /groups/riss/onson001/split/output

hadoop jar /panfs/roc/itascasoft/hadoop/2.7.1/share/hadoop/tools/lib/hadoop-streaming-2.7.1.jar \
-files "test.sh,11-05910_R1.fastq,forward.txt" \
-numReduceTasks 0 \
-cmdenv R1="11-05910_R1.fastq" \
-cmdenv forward="forward.txt" \
-inputformat org.apache.hadoop.mapred.lib.NLineInputFormat \
-mapper test.sh \
-input /groups/riss/onson001/split/fastq_files.txt \
-output /groups/riss/onson001/split/output

hadoop fs -put test.txt  /hdfs/onson001/test.txt >&2 || { echo "put failed" >&2; exit 1; }

"hadoop fs -ls /groups/riss/onson001/split/*"

hadoop fs -cat /groups/riss/onson001/split/output/part-00000

                                          


                                          
                                          
                                          
                                          
                                          
                                          
                                          
                                          
                                          
                                          
                                          
                                          
                                          
                                          
                                          
                                          
                                          
                                          







/home/thyagara/data_release/umgc/hiseq/131202_700506R_0311_AH79LPADXX/Project_UMGC_Project_147










*** PLOT TO SEE IF EVERYTHING IS WORKING

















# Compile Class to Tag data and create Jar file
cd TagData/
hadoop com.sun.tools.javac.Main TagData.java
jar cf TagData.jar *.class


# Tag pileup data
# Data tagged with n will be numerator while data tagged as d will be denominator in a division

cd /Users/onson001/Desktop/hadoop

hadoop fs -rm -r fs_output/control_bwa_no_dup
hadoop fs -rm -r fs_output/control_bowtie
hadoop fs -rm -r fs_output/control_bwa
hadoop fs -rm -r fs_output/sample_bwa_no_dup
hadoop fs -rm -r fs_output/sample_bowtie
hadoop fs -rm -r fs_output/sample_bwa

hadoop jar TagData/TagData.jar TagData fs_data/control_bwa_no_dup.txt fs_output/control_bwa_no_dup d
hadoop jar TagData/TagData.jar TagData fs_data/control_bowtie.txt fs_output/control_bowtie n
hadoop jar TagData/TagData.jar TagData fs_data/control_bwa.txt fs_output/control_bwa d

hadoop jar TagData/TagData.jar TagData fs_data/sample_bwa_no_dup.txt fs_output/sample_bwa_no_dup n
hadoop jar TagData/TagData.jar TagData fs_data/sample_bowtie.txt fs_output/sample_bowtie n
hadoop jar TagData/TagData.jar TagData fs_data/sample_bwa.txt fs_output/sample_bwa d

# Delete output folder if it exists
hadoop fs -rm -r fs_output/SampleOverControlOut
hadoop fs -rm -r fs_output/ControlBowtieBwaOut
hadoop fs -rm -r fs_output/SampleBowtieBwaOut

# Calculate ratios
hadoop jar CoverageRatio/CoverageRatio.jar CoverageRatio fs_output/sample_bwa_no_dup fs_output/control_bwa_no_dup fs_output/SampleOverControlOut
hadoop jar CoverageRatio/CoverageRatio.jar CoverageRatio fs_output/control_bowtie fs_output/control_bwa fs_output/ControlBowtieBwaOut
hadoop jar CoverageRatio/CoverageRatio.jar CoverageRatio fs_output/sample_bowtie fs_output/sample_bwa fs_output/SampleBowtieBwaOut

hadoop fs -rm -r fs_output/tempOut
hadoop jar SecondarySortTextPair/SecondarySortTextPair.jar SecondarySortTextPair fs_data/temp.txt fs_output/tempOut

hadoop fs -rm -r fs_output/tempOut2
hadoop jar SecondarySortTextIntPair/SecondarySortTextIntPair.jar SecondarySortTextIntPair fs_data/temp.txt fs_output/tempOut2

cd /Users/onson001/Desktop/hadoop/Pig

pig -f window_coverage.pig -param cov_data_input='/Users/onson001/Desktop/hadoop/onsongo/temp_cov_data.txt' \
-param window_data_input='/Users/onson001/Desktop/hadoop/onsongo/temp_window_data.txt' \
-param window_coverage_output='/Users/onson001/Desktop/hadoop/onsongo/window_coverage'

hadoop fs -rm -r fs_output/WithinRatioOut

dump A;






hadoop jar CoverageRatio/CoverageRatio.jar CoverageRatio fs_output/sample_bwa_no_dup fs_output/control_bwa_no_dup fs_output/ControlBowtieBwaOut2


hadoop jar CoverageRatio/CoverageRatio.jar CoverageRatio fs_output/control_bowtie fs_output/control_bwa ControlBowtieBwaOut

hadoop jar WordCount.jar WordCount onsongo/test.txt WordCountOut
hadoop jar JoinSampleControl/JoinSampleControl.jar JoinSampleControl onsongo/pah_pileup.txt JoinSampleControlOut
hadoop jar CoverageRatio/CoverageRatio.jar CoverageRatio JoinSampleControlOut CoverageRatioOut
hadoop jar TagData/TagData.jar TagData onsongo/temp.txt TagDataOut c


# Check to see if result are there
hadoop fs -ls  TagDataOut


# Adding classpath to java
javac -classpath ../anc/BigDecimalMath.jar *.java
java -classpath ../anc/BigDecimalMath.jar:. TestSimpleMovingAverage 3





# FAILED ATTEMPT AT USING DISTRIBUTED CACHE

# Copy files to HDFS so we can set it up for distibuted cache
hadoop fs -copyFromLocal raw_data/reference_pileup.txt fs_data/reference_pileup.txt

-- HERE

# PUT reference_pileup.txt INTO DISTRIBUTED CACHE SO WE DON'T NEED TO DISTRIBUTE IT

#2. Setup the application's JobConf:

// Mind the # sign after the absolute file location.
// You will be using the name after the # sign as your
// file name in your Mapper/Reducer
JobConf job = new JobConf();
DistributedCache.addCacheFile(new URI("fs_data/reference_pileup.txtreference_pileup_path"),job);



3. Use the cached files in the Mapper
or Reducer:

public static class MapClass extends MapReduceBase
implements Mapper<K, V, K, V> {
    
    private Path[] localArchives;
    private Path[] localFiles;
    
    public void configure(JobConf job) {
        // Get the cached archives/files
        localArchives = DistributedCache.getLocalCacheArchives(job);
        localFiles = DistributedCache.getLocalCacheFiles(job);
    }
