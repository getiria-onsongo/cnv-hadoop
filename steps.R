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

