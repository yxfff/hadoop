#!/bin/bash
user_id=$1
movie_id=$2

hadoop jar  /data/MovieProject/queryhbase/QueryHbase/target/QueryHbase-0.0.1-SNAPSHOT.jar com.xfyan.query.QueryHbase $user_id $movie_id

