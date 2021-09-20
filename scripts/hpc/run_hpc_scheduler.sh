#!/bin/bash
#PBS -N airflow-scheduler
#PBS -o /dev/null
#PBS -e /dev/null
#PBS -l walltime=01:00:00
#PBS -l select=1:ncpus=4:mem=4gb

source /rds/general/user/igf/home/data2/airflow_test/secrets/hpc_env.sh

airflow scheduler --pid ${TMPDIR}