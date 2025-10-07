#!/bin/bash
mkdir /home/temp_files
nohup node index.js > output.log 2>&1 &

