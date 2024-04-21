#!/bin/bash


../scripts/env.sh

hdfs dfs -appendToFile ../file/local.txt text.txt
hdfs dfs -mv text.txt text3.txt