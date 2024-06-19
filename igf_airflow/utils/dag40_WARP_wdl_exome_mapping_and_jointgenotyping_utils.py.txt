import os
import json
import pendulum
import logging
import shutil
import pandas as pd
from airflow import XComArg
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context
from igf_data.utils.bashutils import bash_script_wrapper
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from igf_data.utils.analysis_fastq_fetch_utils import get_fastq_and_run_for_samples
from igf_data.utils.seqrunutils import get_seqrun_date_from_igf_id
from igf_airflow.utils.generic_airflow_utils import (
    send_airflow_failed_logs_to_channels,
    send_airflow_pipeline_logs_to_channels)
from igf_data.utils.fileutils import (
  check_file_path,
  copy_local_file,
  get_temp_dir,
  get_date_stamp)
from igf_airflow.utils.dag22_bclconvert_demult_utils import (
  _create_output_from_jinja_template)
from igf_airflow.utils.dag26_snakemake_rnaseq_utils import (
  parse_analysis_design_and_get_metadata,
  get_project_igf_id_for_analysis)

## BASE
DATABASE_CONFIG_FILE = Variable.get('database_config_file', default_var=None)
HPC_FILE_LOCATION = Variable.get("hpc_file_location", default_var="HPC_PROJECT")
## NOTIFICATION
SLACK_CONF = Variable.get('analysis_slack_conf',default_var=None)
MS_TEAMS_CONF = Variable.get('analysis_ms_teams_conf',default_var=None)
## GLOBUS
GLOBUS_ROOT_DIR = Variable.get("globus_root_dir", default_var=None)
## EMAIL CONFIG
EMAIL_CONFIG = Variable.get("email_config", default_var=None)
EMAIL_TEMPLATE = Variable.get("analysis_email_template", default_var=None)
## WDL PIPELINE CONF
EXOME_SAMPLE_INPUT_TEMPLATE = "/rds/general/project/genomics-facility-archive-2019/ephemeral/cromwell_test/wdl_templates/sample_input.json"
EXOME_WDL_CMD_TEMPLATE = "/rds/general/project/genomics-facility-archive-2019/ephemeral/cromwell_test/wdl_templates/exome_cmd.sh"
GENOTYPE_SAMPLE_INPUT_TEMPLATE = "/rds/general/project/genomics-facility-archive-2019/ephemeral/cromwell_test/wdl_templates/sample_input.json"
GENOTYPE_WDL_CMD_TEMPLATE = "/rds/general/project/genomics-facility-archive-2019/ephemeral/cromwell_test/wdl_templates/exome_cmd.sh"

log = logging.getLogger(__name__)

@task(
  task_id="create_json_input_for_analysis_design",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G',
  multiple_outputs=False)
def create_json_input_for_analysis_design(analysis_design: dict) -> str:
  try:
    sample_input_json_file = \
      create_json_input_for_ubam_conversion(
        analysis_design=analysis_design,
        db_config_file=DATABASE_CONFIG_FILE)
    return sample_input_json_file
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


def create_json_input_for_ubam_conversion(
      analysis_design: dict,
      db_config_file: str) -> str:
  try:
    analysis_design_yaml = \
      analysis_design.get("analysis_design")
    if analysis_design_yaml is None:
      raise KeyError(
        f"Missing analysis_design in the dictionary {analysis_design}")
    ## check if file exists
    check_file_path(analysis_design_yaml)
    with open(analysis_design_yaml, 'r') as fp:
      input_design_yaml = fp.read()
    sample_metadata, analysis_metadata = \
      parse_analysis_design_and_get_metadata(
        input_design_yaml=input_design_yaml)
    if sample_metadata is None or \
       analysis_metadata is None:
      raise KeyError("Missing sample or analysis metadata")
    ## get sample ids from metadata
    sample_igf_id_list = \
      list(sample_metadata.keys())
    if len(sample_igf_id_list) == 0:
      raise ValueError("No sample id found in the metadata")
    fastq_info_list = \
      get_fastq_and_run_for_samples(
        dbconfig_file=db_config_file,
        sample_igf_id_list=sample_igf_id_list)
    ## convert fastq list to input json
    df = pd.DataFrame(fastq_info_list)
    final_output_json_list = list()
    for (sample_igf_id, run_igf_id), r_data in df.groupby(["sample_igf_id", "run_igf_id"]):
      fastqs = r_data['file_path'].values.tolist()
      r1_file = [f for f in fastqs if f.endswith("R1_001.fastq.gz")][0]
      r2_file = [f for f in fastqs if f.endswith("R2_001.fastq.gz")][0]
      model_name = r_data["model_name"].values[0]
      platform_igf_id = r_data["platform_igf_id"].values[0]
      platform_name = r_data["vendor_name"].values[0]
      seqrun_igf_id = r_data["seqrun_igf_id"].values[0]
      seqrun_date = \
        get_seqrun_date_from_igf_id(seqrun_igf_id)
      final_output_json_list.append({
        "readgroup_name": run_igf_id,
        "sample_name": sample_igf_id,
        "fastq_1": r1_file,
        "fastq_2": r2_file,
        "library_name": f"{sample_igf_id}_{model_name}",
        "platform_unit": platform_igf_id,
        "run_date": seqrun_date,
        "platform_name": platform_name,
        "sequencing_center": "IGF"
      })
    temp_work_dir = \
      get_temp_dir(use_ephemeral_space=True)
    sample_input_json_file = \
      os.path.join(temp_work_dir, "sample_input.json")
    with open(sample_input_json_file, "w") as fp:
      json.dump(final_output_json_list, fp)
    return sample_input_json_file
  except Exception as e:
    raise ValueError(
      f"Failed to create ubam conversion input list, error: {e}")

## TASK read and create dynamic task list
@task(
  task_id="read_input_json_for_ubam_conversion",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G',
  multiple_outputs=False)
def read_input_json_for_ubam_conversion(input_json_file: str) -> list:
  try:
    check_file_path(input_json_file)
    with open(input_json_file, 'r') as fp:
      json_data = json.load(fp)
    return json_data
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)

## TASK
@task(
  task_id="collect_ubams_after_conversion",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G',
  multiple_outputs=False)
def collect_ubams_after_conversion(ubam_list: list) -> dict:
  try:
    ubam_output_list = list()
    for entry in ubam_list:
      unmapped_bam = entry.get("unmapped_bam")
      sample_name = entry.get("sample_name")
      if unmapped_bam is not None and \
         sample_name is not None:
        ubam_output_list.append({
          "sample_name": sample_name,
          "unmapped_bam": unmapped_bam})
      per_sample_ubams = \
        pd.DataFrame(ubam_output_list).\
        groupby('sample_name', as_index=False).\
        agg({"unmapped_bam": list}).\
        to_dict(orient="records")
      return per_sample_ubams
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)