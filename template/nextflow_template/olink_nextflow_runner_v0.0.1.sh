#!/bin/bash
## INPUT:
# * WORKDIR
# * CONFIG_FILE
## ENVS
source /rds/general/project/genomics-facility-archive-2019/live/tgu/resources/pipeline_resource/nextflow/env.sh

export TMPDIR=$EPHEMERAL
export NXF_OPTS='-Xms1g -Xmx4g'

cd {{ WORKDIR }}

$NEXTFLOW_EXE run /rds/general/project/genomics-facility-archive-2019/live/OLINK_DATA/olink_reveal_npx_and_qc_pipeline/main.nf \
  -with-tower "$NEXTFLOW_TOWER" \
  -resume \
  -profile singularity \
  --outdir {{ WORKDIR }}/results \
  -work-dir {{ WORKDIR }}/work \
  -with-report {{ WORKDIR }}/results/report.html \
  -with-dag  {{ WORKDIR }}/results/dag.html \
  -with-timeline {{ WORKDIR }}/results/timeline.html \
  -config {{ CONFIG_FILE }}