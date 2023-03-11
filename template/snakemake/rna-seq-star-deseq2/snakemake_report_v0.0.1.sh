#!/bin/bash

## THIS IS A TEMPLATE FOR SNAKEMAKE RNASEQ REPORT GENERATION PIPELINE
## VERSION: 0.0.1
##
## REQUIRED INPUTS:
##
##  * CONFIG_YAML_PATH: Path to the dynamically generated `config.yaml` file for the run
##  * SNAKEMAKE_WORK_DIR: Temp work dir for pipeline run
##


## LOAD ENV
module load anaconda3/personal
source activate snakemake

## SET PIPELINE CONF
SNAKEFILE=/project/tgu/resources/pipeline_resource/snakemake/workflow/rna-seq-star-deseq2/workflow/Snakefile


## GO TO WORKDIR
cd {{ SNAKEMAKE_WORK_DIR }}

snakemake \
  --configfile {{ CONFIG_YAML_PATH }} \
  --snakefile $SNAKEFILE \
  --report results/report.html



