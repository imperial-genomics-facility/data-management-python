## ENVS
source /rds/general/project/genomics-facility-archive-2019/live/tgu/resources/pipeline_resource/nextflow/curioseeker_env.sh

export TMPDIR=$EPHEMERAL
export NXF_OPTS='-Xms1g -Xmx4g'

cd {{ WORKDIR }}

$NEXTFLOW_EXE run /rds/general/project/genomics-facility-archive-2019/live/OLINK_DATA/olink_reveal_npx_and_qc_pipeline/main.nf \
  -with-tower $NEXTFLOW_TOWER \
  -resume \
  -profile singularity \
  --outdir {{ OUTPUT_DIR }} \
  -work-dir {{ WORKDIR }}/work \
  -with-report {{ OUTPUT_DIR }}/report.html \
  -with-dag  {{ OUTPUT_DIR }}/dag.html \
  -with-timeline {{ OUTPUT_DIR }}/timeline.html \
  -config {{ CONFIG_FILE }}