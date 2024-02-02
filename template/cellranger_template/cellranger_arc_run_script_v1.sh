#!/bin/bash

## THIS IS A TEMPLATE FOR RUNNING CELLRANGER ARC PIPELINE
## VERSION: 0.0.1
##
## REQUIRED INPUTS:
##
##  * CELLRANGER_ARC_ID: Cellranger ID
##  * CELLRANGER_ARC_CSV: Cellranger arc count csv
##  * CELLRANGER_ARC_CONFIG_PARAMS: Cellranger arc count config params
##  * CELLRANGER_ARC_REFERENCE: Reference genome path
##  * WORKDIR: Work dir path
##
## ENV VARS:
##  * CELLRANGER_MAX_JOB_COUNTS
##  * CELLRANGER_ARC_EXE
##

## IMPORT ENV
source /project/tgu/resources/pipeline_resource/cellranger/env.sh

cd {{ WORKDIR }}

## CELLRANGER ARC COUNT RUN CMD
$CELLRANGER_ARC_EXE count \
  --id={{ CELLRANGER_ARC_ID }} \
  --libraries={{ CELLRANGER_ARC_CSV }} \
  --maxjobs=${CELLRANGER_MAX_JOB_COUNTS} \
  --localcores=4 \
  --localmem=8 \
  --jobmode=pbspro \
  --disable-ui \
  --reference={{ CELLRANGER_ARC_REFERENCE }} {{ CELLRANGER_ARC_CONFIG_PARAMS }}