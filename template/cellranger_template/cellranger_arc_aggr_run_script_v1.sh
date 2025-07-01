#!/bin/bash

## THIS IS A TEMPLATE FOR RUNNING CELLRANGER ARC PIPELINE
## VERSION: 0.0.1
##
## REQUIRED INPUTS:
##
##  * CELLRANGER_ARC_AGGR_ID: Cellranger ID
##  * CELLRANGER_ARC_CSV: Cellranger arc count csv
##  * CELLRANGER_ARC_AGGR_CONFIG: Cellranger arc count config
##  * CELLRANGER_ARC_AGGR_REFERENCE: Reference genome path
##  * CELLRANGER_ARC_AGGR_PARAMS: Optional extra params
##  * WORKDIR: Work dir path
##
## ENV VARS:
##  * CELLRANGER_MAX_JOB_COUNTS
##  * CELLRANGER_ARC_EXE
##

## IMPORT ENV
source /rds/general/project/genomics-facility-archive-2019/live/tgu/resources/pipeline_resource/cellranger/env.sh

cd {{ WORKDIR }}

## CELLRANGER ARC AGGR RUN CMD
$CELLRANGER_ARC_EXE aggr \
  --id={{ CELLRANGER_ARC_AGGR_ID }} \
  --csv={{ CELLRANGER_ARC_AGGR_CSV }} \
  --maxjobs=${CELLRANGER_MAX_JOB_COUNTS} \
  --localcores=4 \
  --localmem=8 \
  --jobmode=pbspro \
  --disable-ui \
  --reference={{ CELLRANGER_ARC_AGGR_REFERENCE }} {{ CELLRANGER_ARC_AGGR_PARAMS }}