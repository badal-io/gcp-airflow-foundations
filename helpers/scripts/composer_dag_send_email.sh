#!/bin/bash

# - Wait until the DAG is initialized
n=0
until [[ $n -ge $4 ]]
do
  status=0
  gcloud beta composer environments run $1 --location $2 dags list 2>&1 | grep $3 && break
  status=$?
  n=$(($n+1))
  sleep $5
done

# - Unpause the DAG if paused
gcloud beta composer environments run $1 --location $2 dags unpause -- $3

# - Run the DAG "send_email". The email contains commit ID (passed as a parameter)
gcloud beta composer environments run $1 --location $2 dags trigger -- $3 --conf '{"commit-id":"'${6}'"}'

exit $status
