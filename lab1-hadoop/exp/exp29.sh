#!/bin/bash


../scripts/env.sh

hdfs dfs -rm text.txt

hdfs dfs -ls -h text.txt