#!/bin/bash

job_queue=${1:?'Missing job queue'}
job_name=${2:?'Missing job name'}

source /rds/general/project/genomics-facility-archive-2019/live/AIRFLOW/airflow_v4/secrets/hpc_env.sh
cd /rds/general/user/igf/ephemeral
mkdir -p /rds/general/user/igf/ephemeral/${PBS_JOBID}

airflow celery worker \
  --pid /rds/general/user/igf/ephemeral/${PBS_JOBID}/pid \
  --celery-hostname ${PBS_JOBID}-${job_name} \
  --queues ${job_queue} \
  --skip-serve-logs
