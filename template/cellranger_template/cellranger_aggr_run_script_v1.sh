#!/bin/bash

## THIS IS A TEMPLATE FOR RUNNING CELLRANGER AGGR PIPELINE
## VERSION: 0.0.1
##
## REQUIRED INPUTS:
##
##  * CELLRANGER_AGGR_ID: Cellranger ID
##  * CELLRANGER_AGGR_CSV: Cellranger aggr csv
##  * CELLRANGER_AGGR_OUTPUT_DIR: Cellranger aggr output dir
##  * WORKDIR: Work dir path
##
## ENV VARS:
##  * CELLRANGER_MAX_JOB_COUNTS
##  * CELLRANGER_EXE
##

## IMPORT ENV
source /rds/general/project/genomics-facility-archive-2019/live/tgu/resources/pipeline_resource/cellranger/env.sh

cd {{ WORKDIR }}

## CELLRANGER MULTI RUN CMD
$CELLRANGER_EXE aggr \
  --id={{ CELLRANGER_AGGR_ID }} \
  --csv={{ CELLRANGER_AGGR_CSV }} \
  --normalize=mapped \
  --output-dir={{ CELLRANGER_AGGR_OUTPUT_DIR }} \
  --maxjobs=${CELLRANGER_MAX_JOB_COUNTS} \
  --localcores=4 \
  --localmem=8 \
  --jobmode=pbspro \
  --disable-ui