#!/bin/bash

javac WordCount.java -cp $(/data/ywxia/hadoop-3.1.3/bin/hadoop classpath)
# hadoop jar WordCount.jar org/apache/hadoop/examples/WordCount input output
jar -cvf WordCount.jar ./WordCount*.class

hadoop fs -put ./input input 
hadoop jar WordCount.jar org/apache/hadoop/examples/WordCount input output
hdfs dfs -cat output/part-r-00000