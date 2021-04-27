#!/bin/bash
set -e

export PROJECT=<yourprojectname>
export DATASET=ms_poc
export LOCATION=europe-west2
export sql_file=pop_ms_query.sql
n=1

while [ $n -le 20 ]
do
echo $n
cat "$sql_file" \
      | bq \
        --project_id=${PROJECT} \
        --dataset_id=${DATASET} \
        query \
        --location=${LOCATION} \
        --use_cache=false \
        --use_legacy_sql=false \
        --batch=false \
        --append_table \
        --destination_table=ms_poc.markerstudy_data \
        --max_rows=0 \
        --allow_large_results
n=$(( n+1 ))
done