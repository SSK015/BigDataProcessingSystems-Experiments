#!/bin/bash


../scripts/env.sh

hdfs dfs -mkdir -p /user/xyw/heptagon
hdfs dfs -rmdir /user/xyw/heptagon
# hdfs dfs -rm -R /user/xyw/heptagon