#!/bin/bash
#PBS -N airflow-scheduler
#PBS -o /dev/null
#PBS -e /dev/null
#PBS -l walltime=01:00:00
#PBS -l select=1:ncpus=4:mem=4gb

source /rds/general/project/genomics-facility-archive-2019/live/AIRFLOW/airflow_v4/secrets/hpc_env.sh
cd /rds/general/user/igf/ephemeral
mkdir -p /rds/general/user/igf/ephemeral/${PBS_JOBID}

airflow scheduler --pid /rds/general/user/igf/ephemeral/${PBS_JOBID}/pid