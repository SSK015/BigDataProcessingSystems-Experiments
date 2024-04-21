#!/bin/bash

../scripts/env.sh

hdfs dfs -rm text.txt

hdfs dfs -touchz text.txt
hdfs dfs -test -e text.txt


hdfs dfs -appendToFile ../file/local.txt text.txt
hdfs dfs -appendToFile ../file/local.txt text.txt

hdfs dfs -cat /user/$(whoami)/text.txt

hdfs dfs -copyFromLocal -f ../file/overwrite.txt text.txt

hdfs dfs -cat /user/$(whoami)/text.txt
# hdfs dfs -copyFromLocal -f ../file/local.txt text.txt