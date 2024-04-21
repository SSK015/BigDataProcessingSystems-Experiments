#!/bin/bash

for ((i=1; i<=11; i++)); do
    echo -e "Task $i:\n"
    ./exp2${i}.sh
    echo -e "\n"
done