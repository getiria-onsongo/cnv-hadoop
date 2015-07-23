# Start hadoop
hstart

# Upload files to Hadoop file System
hadoop fs -put raw_data/control_bwa_no_dup.txt fs_data/control_bwa_no_dup.txt
hadoop fs -put raw_data/control_bowtie.txt fs_data/control_bowtie.txt
hadoop fs -put raw_data/control_bwa.txt fs_data/control_bwa.txt

hadoop fs -put raw_data/sample_bwa_no_dup.txt fs_data/sample_bwa_no_dup.txt
hadoop fs -put raw_data/sample_bowtie.txt fs_data/sample_bowtie.txt
hadoop fs -put raw_data/sample_bwa.txt fs_data/sample_bwa.txt


# Compile Class to Tag data and create Jar file
cd TagData/
hadoop com.sun.tools.javac.Main TagData.java
jar cf TagData.jar *.class


# Tag pileup data
# Data tagged with n will be numerator while data tagged as d will be denominator in a division

hadoop jar TagData/TagData.jar TagData fs_data/control_bwa_no_dup.txt fs_output/control_bwa_no_dup d
hadoop jar TagData/TagData.jar TagData fs_data/control_bowtie.txt fs_output/control_bowtie n
hadoop jar TagData/TagData.jar TagData fs_data/control_bwa.txt fs_output/control_bwa d

hadoop jar TagData/TagData.jar TagData fs_data/sample_bwa_no_dup.txt fs_output/sample_bwa_no_dup n
hadoop jar TagData/TagData.jar TagData fs_data/sample_bowtie.txt fs_output/sample_bowtie n
hadoop jar TagData/TagData.jar TagData fs_data/sample_bwa.txt fs_output/sample_bwa d

# Delete output folder if it exists
hadoop fs -rm -r fs_output/outputFolder

# Calculate ratios
hadoop jar CoverageRatio/CoverageRatio.jar CoverageRatio fs_output/sample_bwa_no_dup fs_output/control_bwa_no_dup fs_output/SampleOverControlOut
hadoop jar CoverageRatio/CoverageRatio.jar CoverageRatio fs_output/control_bowtie fs_output/control_bwa fs_output/ControlBowtieBwaOut
hadoop jar CoverageRatio/CoverageRatio.jar CoverageRatio fs_output/sample_bowtie fs_output/sample_bwa fs_output/SampleBowtieBwaOut



--- HERE







hadoop jar CoverageRatio/CoverageRatio.jar CoverageRatio fs_output/sample_bwa_no_dup fs_output/control_bwa_no_dup fs_output/ControlBowtieBwaOut2


hadoop jar CoverageRatio/CoverageRatio.jar CoverageRatio fs_output/control_bowtie fs_output/control_bwa ControlBowtieBwaOut

hadoop jar WordCount.jar WordCount onsongo/test.txt WordCountOut
hadoop jar JoinSampleControl/JoinSampleControl.jar JoinSampleControl onsongo/pah_pileup.txt JoinSampleControlOut
hadoop jar CoverageRatio/CoverageRatio.jar CoverageRatio JoinSampleControlOut CoverageRatioOut
hadoop jar TagData/TagData.jar TagData onsongo/temp.txt TagDataOut c


# Check to see if result are there
hadoop fs -ls  TagDataOut

