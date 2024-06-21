#!/bin/bash

## THIS IS A TEMPLATE FOR RUNNING SPACERANGER COUNT PIPELINE
## VERSION: 0.0.1
##
## REQUIRED INPUTS:
##
##  * SPACERANGER_ID: Spaceranger ID
##  * SPACERANGER_PARAMS: Spaceranger count parameters
##  * WORKDIR: Work dir path
##
## ENV VARS:
##  * SPACERANGER_MAX_JOB_COUNTS
##  * SPACERANGER_EXE
##

## IMPORT ENV
source /project/tgu/resources/pipeline_resource/spaceranger/env.sh

cd {{ WORKDIR }}

## SPACERANGER COUNT RUN CMD
$SPACERANGER_EXE count \
  --id={{ SPACERANGER_ID }} \
  --fastqs={{ FASTQS }} \
  --localcores=4 \
  --localmem=8 \
  --jobmode=pbspro \
  --disable-ui \
  --maxjobs=${SPACERANGER_MAX_JOB_COUNTS} \
  {{ SPACERANGER_PARAMS }}