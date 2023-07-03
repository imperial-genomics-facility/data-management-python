#!/bin/bash

job_queue=${1:?'Missing job queue'}
job_name=${2:?'Missing job name'}

source /rds/general/user/igf/home/data2/airflow_v3/secrets/hpc_env.sh

airflow celery worker \
  --pid $TMPDIR/pid \
  --celery-hostname ${PBS_JOBID}-${job_name} \
  --queues ${job_queue} \
  --skip-serve-logs
