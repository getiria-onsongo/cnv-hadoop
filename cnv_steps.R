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
java TestSimpleMovingAverage 5


# NEXT: Create a class that takes in array and uses SimpleMovingAverageUsingArray.java to compute moving average
# returning an array of moving average values


# Rolling mean (window = 200)
# if(window_length >= gene_length/4)
#   window_length = round(gene_length/4)
# if(window_length < 1)
#     window_length = 1
# NOTE: smoothing is done on both ratio and bowtie/bwa
R CMD BATCH smooth_coverage.R


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
