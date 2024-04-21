#!/bin/bash


../scripts/env.sh

hdfs dfs -appendToFile ../file/local.txt text.txt

hdfs dfs -cat text.txt