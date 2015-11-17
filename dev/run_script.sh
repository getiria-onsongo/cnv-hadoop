#!/usr/bin/env bash

hadoop fs -rm -r fastq

hadoop fs -mkdir fastq

sh SplitFastqSingleNode.sh input.txt fastq 10000 heart

hadoop com.sun.tools.javac.Main BwaMapping.java
jar cf BwaMapping.jar *.class
hadoop jar BwaMapping.jar BwaMapping heart BwaMappingOut "genomes/bwa.tar.gz" "fastq" 10
