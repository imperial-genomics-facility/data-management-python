#!/bin/bash

## THIS IS A TEMPLATE FOR RUNNING NF-CORE PIPELINE
## VERSION: 0.0.1
##
## REQUIRED INPUTS:
##
##  * NEXTFLOW_PARAMS: Nextflow params
##  * NEXTFLOW_CONF: Nextflow config file for the run
##  * NEXTFLOW_VERSION: Nextflow version for the tool
##  * NFCORE_PIPELINE_NAME: NF-Core pipeline name
##  * WORKDIR: Work dir path

## IMPORT ENV
source /rds/general/project/genomics-facility-archive-2019/live/tgu/resources/pipeline_resource/nextflow/env.sh

cd {{ WORKDIR }}

## SET NXF_VER FOR PIPELINE RUN
export NXF_VER={{ NEXTFLOW_VERSION }}
export NXF_OPTS='-Xms1g -Xmx4g'

## NEXTFLOW RUN CMD
$NEXTFLOW_EXE run {{ NFCORE_PIPELINE_NAME }} \
  -with-tower $NEXTFLOW_TOWER \
  -c {{ NEXTFLOW_CONF }} \
  {{ NEXTFLOW_PARAMS }}