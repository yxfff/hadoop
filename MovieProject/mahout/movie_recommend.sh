#!/bin/bash

cd /data/ml-1m/ &&
tr -s '::' ','<ratings.dat | cut -f1-3 -d,> rating.csv
tr -s '::' ','<users.dat | cut -f1-3 -d,> users.csv

hadoop fs -put rating.csv /input
hadoop fs -put users.csv /input

cd /usr/lib/mahout && bin/mahout recommenditembased --similarityClassname SIMILARITY_PEARSON_CORRELATION \
				--numRecommendations 2 \
				--usersFile /input/users.csv \
				--input /input/rating.csv \
				--output output/ \
				--tempDir output/temp \


