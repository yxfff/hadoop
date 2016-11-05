#!/bin/bash

gender=$1
age=$2
output=$3

hadoop jar /data/MovieProject/queryhbase/QueryHbase/target/QueryHbase-0.0.1-SNAPSHOT.jar com.xfyan.query.MRQueryHbase $gender $age $output

