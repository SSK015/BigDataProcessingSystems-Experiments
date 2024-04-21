#!/bin/bash


../scripts/env.sh

if $(hdfs dfs -test -d /user/xyw/exp26);
then $(hdfs dfs -touchz /user/xyw/exp26/hello.txt); 
else $(hdfs dfs -mkdir -p  /user/xyw/exp26/ && hdfs dfs -touchz /user/xyw/exp26/hello.txt); 
fi

hdfs dfs -ls -R -h /user/xyw/exp26