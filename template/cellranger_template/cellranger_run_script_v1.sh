#!/bin/bash

## THIS IS A TEMPLATE FOR RUNNING CELLRANGER PIPELINE
## VERSION: 0.0.1
##
## REQUIRED INPUTS:
##
##  * CELLRANGER_MULTI_ID: Cellranger ID
##  * CELLRANGER_MULTI_CSV: Cellranger multi csv
##  * CELLRANGER_MULTI_OUTPUT_DIR: Cellranger multi output dir
##  * WORKDIR: Work dir path

## IMPORT ENV
source /project/tgu/resources/pipeline_resource/cellranger/env.sh

cd {{ WORKDIR }}

## CELLRANGER MULTI RUN CMD
$CELLRANGER_EXE multi \
  --id={{ CELLRANGER_MULTI_ID }} \
  --csv={{ CELLRANGER_MULTI_CSV }} \
  --output-dir={{ CELLRANGER_MULTI_OUTPUT_DIR }} \
  --maxjobs=${CELLRANGER_MULTI_JOB_COUNTS} \
  --localcores=4 \
  --localmem=8 \
  --jobmode=pbspro \
  --disable-ui