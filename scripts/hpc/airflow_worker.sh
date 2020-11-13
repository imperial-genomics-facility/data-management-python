#!/bin/bash

job_queue=${1:?'Missing job queue'}
job_name=${2:?'Missing job name'}

source /rds/general/user/igf/home/data2/airflow_test/secrets/hpc_env.sh

airflow worker \
  --pid $TMPDIR \
  -cn ${PBS_JOBID}-${job_name} \
  -q ${job_queue} \
  --skip_serve_logs \
  --log-file ${AIRFLOW__CORE__BASE_LOG_FOLDER}
