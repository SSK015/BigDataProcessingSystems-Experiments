#!/bin/bash

# Please put your .txt file under hdfs/pagerank

javac PageRank.java -cp $(hadoop classpath)

jar -cvf pagerank.jar ./PageRank*.class

hadoop jar pagerank.jar PageRank -i pagerank -o out