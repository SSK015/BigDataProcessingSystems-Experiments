#!/bin/bash

../scripts/env.sh

if $(hdfs dfs -test -e text.txt);
then $(hdfs dfs -copyToLocal text.txt ./text2.txt); 
else $(hdfs dfs -copyToLocal text.txt ./text.txt); 
fi

cat text2.txt
cat text.txt