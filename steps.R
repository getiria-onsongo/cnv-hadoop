# After installing hadoop, add these start and stop short hands

alias hstart="/usr/local/Cellar/hadoop/2.6.0/sbin/start-dfs.sh;/usr/local/Cellar/hadoop/2.6.0/sbin/start-yarn.sh"
alias hstop="/usr/local/Cellar/hadoop/2.6.0/sbin/stop-yarn.sh;/usr/local/Cellar/hadoop/2.6.0/sbin/stop-dfs.sh"

# Add these paths in .bash_profile 

export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.7.0_45.jdk/Contents/Home
export PATH=${JAVA_HOME}/bin:${PATH}
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar

# Compile code
hadoop com.sun.tools.javac.Main CoverageRatio.java

hadoop com.sun.tools.javac.Main WordCount.java
hadoop com.sun.tools.javac.Main JoinSampleControl.java
hadoop com.sun.tools.javac.Main TagData.java

# Create jar
jar cf CoverageRatio.jar *.class

jar cf WordCount.jar *.class
jar cf JoinSampleControl.jar *.class
jar cf TagData.jar *.class

# Create folder
hadoop fs -mkdir onsongo
hadoop fs -mkdir fs_data

# Put sample file 
hadoop fs -put test.txt onsongo/test.txt
hadoop fs -put pah_pileup.txt onsongo/pah_pileup.txt
hadoop fs -put raw_data/temp.txt onsongo/temp.txt


# Check to confirm it was uploaded 
hadoop fs -ls onsongo

# Delete output folder if it exists
hadoop fs -rm -r WordCountOut
hadoop fs -rm -r JoinSampleControlOut
hadoop fs -rm -r CoverageRatioOut

# Run application
hadoop jar WordCount.jar WordCount onsongo/test.txt WordCountOut
hadoop jar JoinSampleControl/JoinSampleControl.jar JoinSampleControl onsongo/pah_pileup.txt JoinSampleControlOut
hadoop jar CoverageRatio/CoverageRatio.jar CoverageRatio JoinSampleControlOut CoverageRatioOut
hadoop jar TagData/TagData.jar TagData onsongo/temp.txt TagDataOut c


# Check to see if result are there
hadoop fs -ls  TagDataOut


# SECONDARY SORT
- Assume we want to sort <year temperature> by temperature (secondary sort)

- Map Reduce framework automatically sorts on key. We can create a composite key but then have the partitioner
to partition by the year part of the key. This will guarantee records for the same year go to the same
reducer.

- Sending records for the same year to the same reducer will not guarantee the secondary sort because
the reducer groups by key and our composite key is of (year,temp). We will need to change how the
grouping is done.

- In summary, we need the following:

1) Make the key be composite of natural key and natural value
2) The sort comparator should order by the composite key
3) The partitioner and grouping comparator for the composite key should consider only the narural key for
grouping and partitioning










