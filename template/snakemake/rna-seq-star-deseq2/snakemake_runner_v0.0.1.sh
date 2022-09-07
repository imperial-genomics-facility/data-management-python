#!/bin/bash

"""
THIS IS A TEMPLATE FOR RUNNING SNAKEMAKE RNASEQ PIPELINE
VERSION: 0.0.1

REQUIRED INPUTS:

  * SINGULARITY_BIND_DIRS: Comma separated paths to bind to singularity container
  * CONFIG_YAML_PATH: Path to the dynamically generated `config.yaml` file for the run
  * SNAKEMAKE_WORK_DIR: Temp work dir for pipeline run

"""

## LOAD ENV
module load anaconda3/personal
source activate snakemake

## SET CACHE DIR
export SNAKEMAKE_OUTPUT_CACHE=/project/tgu/resources/pipeline_resource/snakemake/cache_dir

## SET PIPELINE CONF
SNAKEFILE=/project/tgu/resources/pipeline_resource/snakemake/workflow/rna-seq-star-deseq2/workflow/Snakefile
CONDA_PREFIX_DIR=/project/tgu/resources/pipeline_resource/snakemake/conda_prefix_dir
SINGULARITY_PREFIX_DIR=/project/tgu/resources/pipeline_resource/snakemake/singularity_prefix_dir
CLUSTER_CONFIG_JSON=/project/tgu/resources/pipeline_resource/snakemake/cluster_config/rna-seq-star-deseq2_cluster.json
HPC_QUEUE_CONFIG="qsub -V -o /dev/null -e /dev/null -lwalltime=08:00:00  -lselect=1:ncpus={cluster.ncpus}:mem={cluster.mem}gb"
JOB_LIMIT=20

## GO TO WORKDIR
cd {{ SNAKEMAKE_WORK_DIR }}

snakemake \
  --use-singularity \
  --use-conda \
  --singularity-args="-B {{ SINGULARITY_BIND_DIRS }},$EPHEMERAL:/tmp,$EPHEMERAL:/var/tmp" \
  --cluster-config $CLUSTER_CONFIG_JSON \
  -j $JOB_LIMIT \
  --cluster $HPC_QUEUE_CONFIG \
  --rerun-incomplete \
  --configfile {{ CONFIG_YAML_PATH }} \
  --snakefile $SNAKEFILE \
  --conda-prefix $CONDA_PREFIX_DIR \
  --singularity-prefix $SINGULARITY_PREFIX_DIR \
  --cache \
  --latency-wait 60



