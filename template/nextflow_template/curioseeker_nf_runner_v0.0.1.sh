## REQUIRD VARIABLES
#
# * DIR_LIST
# * WORKDIR
# * SAMPLESHEET_CSV
# * OUTPUT_DIR
# * CONFIG_FILE
# * NEXTFLOW_PARAMS (Optional)
#
## REQUIRED ENVS
#
# * MAX_TASKS
# * NEXTFLOW_EXE
# * CURIOSEEKER_SOFTWARE_PATH
# * NEXTFLOW_TOWER
# * CURIOSEEKER_IGENOME_PATH
# * NEXTFLOW_SINGULARITY_CACHE_DIR
# * CURIOSEEKER_SINGULARIITY_FILE
#
## IMPORT ENV
source /rds/general/project/genomics-facility-archive-2019/live/tgu/resources/pipeline_resource/nextflow/curioseeker_env.sh
export TMPDIR=$EPHEMERAL
## SET NXF_VER FOR PIPELINE RUN
# export NXF_VER={{ NEXTFLOW_VERSION }}
export NXF_OPTS='-Xms1g -Xmx4g'

cd {{ WORKDIR }}

$NEXTFLOW_EXE run $CURIOSEEKER_SOFTWARE_PATH/curioseeker-v3.0.0/main.nf \
  -with-tower $NEXTFLOW_TOWER \
  -resume \
  -profile singularity \
  --igenomes_base $CURIOSEEKER_IGENOME_PATH \
  --input {{ SAMPLESHEET_CSV }} \
  --outdir {{ OUTPUT_DIR }} \
  -work-dir {{ WORKDIR }} \
  -with-report {{ OUTPUT_DIR }}/report.html \
  -with-dag  {{ OUTPUT_DIR }}/dag.html \
  -with-timeline {{ OUTPUT_DIR }}/timeline.html \
  -config {{ CONFIG_FILE }} {{ NEXTFLOW_PARAMS }}
