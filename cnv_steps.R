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



# ----- SEPARATE DATA IN coverage_ratio INTO FIELDS WE CAN JOIN WITH bb_ratio_gene

REGISTER /Users/onson001/Desktop/hadoop/piggybank.jar;
A = LOAD '/Users/onson001/Desktop/hadoop/fs_data/coverage_ratio' USING PigStorage('\t') AS (chr:chararray, coverage_ratio:float);
B = FOREACH A GENERATE REPLACE(chr,';',','),coverage_ratio;
STORE B INTO '/Users/onson001/Desktop/h_test' using PigStorage(',');

C = LOAD '/Users/onson001/Desktop/h_test' USING PigStorage(',') AS (ref_contig:chararray, chr_pos:chararray, coverage_ratio:float);
D = FOREACH C GENERATE ref_contig, REPLACE(chr_pos,':',','),coverage_ratio;
STORE D INTO '/Users/onson001/Desktop/h_test_out' using PigStorage(',');

E = LOAD '/Users/onson001/Desktop/h_test_out' USING PigStorage(',') AS (ref_contig:chararray, chr:chararray, pos:int, coverage_ratio:float);

# LOAD bb_ratio_gene SO WE CAN JOIN

F = LOAD '/Users/onson001/Desktop/hadoop/fs_data/bb_ratio_gene' USING PigStorage('\t') AS (chr:chararray,pos:int,bb_ratio:float,gene_symbol:chararray);

# JOIN E and F

G = JOIN E BY (chr,pos), F BY (chr,pos);
B = FOREACH G GENERATE E::ref_contig AS ref_contig,F::gene_symbol AS gene_symbol,F::chr AS chr,F::pos AS pos,E::coverage_ratio AS coverage_ratio,F::bb_ratio AS bb_ratio;



pig -f Pig/combine_bb_ratio_gene_coverage.pig \
-param coverage_ratio_input='/Users/onson001/Desktop/hadoop/fs_data/coverage_ratio' \
-param bb_ratio_gene_input='/Users/onson001/Desktop/hadoop/fs_data/bb_ratio_gene' \
-param temp1='/Users/onson001/Desktop/hadoop/fs_data/temp1' \
-param temp2='/Users/onson001/Desktop/hadoop/fs_data/temp2' \
-param output='/Users/onson001/Desktop/hadoop/fs_data/combined_data'


# ----

(chr17,7573936,1.0,TP53)


# NEXT:
# 1) Compute sample bowtie/bwa : DONE
# 2) Add bowtie/bwa
# 3) Add gene_symbol
# 4) Use PIG to separate the three reference (Could call three separate
#    pig scripts that take as input master file and one of the references
#    and outputs data for just that reference




# FIELDS WE NEED: chr,pos, ref_exon_contig_id, A_over_B_ratio, bwa_bowtie_ratio, gene_symbol




mysql --socket=$BASE/thesock -u root cnv < create_tables_part1.sql

# WE NOW HAVE ALL THE DATA WE NEED FOR SCALING AND NORMALIZING. LOOK AT FIELDS IN
# cnv_sample_over_control_n_bowtie_bwa_ratio_gene AND PUT THE DATA TOGETHER (LIKELY A PIG OPERATION)


# Normalize data
# a) Find average coverage across genome between 0.5 and 2.0
#           i) all_ratios = 0.5 < ratio < 2.0)
#           ii) avg_log2 = mean(log2(all_ratios))
# b) Normalize in log 2 space
#           i) normalized_value = 2^(log2(value) - avg_log2)
# NOTE: Only ratio is normalized

R CMD BATCH normalize_coverage.R


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
