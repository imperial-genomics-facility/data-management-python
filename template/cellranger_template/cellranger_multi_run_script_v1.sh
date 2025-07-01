#!/bin/bash

## THIS IS A TEMPLATE FOR RUNNING CELLRANGER MULTI PIPELINE
## VERSION: 0.0.1
##
## REQUIRED INPUTS:
##
##  * CELLRANGER_MULTI_ID: Cellranger ID
##  * CELLRANGER_MULTI_CSV: Cellranger multi csv
##  * CELLRANGER_MULTI_OUTPUT_DIR: Cellranger multi output dir
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
$CELLRANGER_EXE multi \
  --id={{ CELLRANGER_MULTI_ID }} \
  --csv={{ CELLRANGER_MULTI_CSV }} \
  --output-dir={{ CELLRANGER_MULTI_OUTPUT_DIR }} \
  --maxjobs=${CELLRANGER_MAX_JOB_COUNTS} \
  --localcores=4 \
  --localmem=8 \
  --jobmode=pbspro \
  --disable-ui