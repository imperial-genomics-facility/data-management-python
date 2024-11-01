import os
import shutil
import logging
from datetime import timedelta
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from igf_airflow.utils.generic_airflow_utils import (
  check_and_seed_analysis_pipeline,
  send_airflow_pipeline_logs_to_channels,
  send_airflow_failed_logs_to_channels,
  generate_email_text_for_analysis,
  calculate_md5sum_for_analysis_dir,
  collect_analysis_dir,
  send_email_via_smtp,
  copy_analysis_to_globus_dir,
  fetch_analysis_yaml_and_dump_to_a_file)
from igf_data.utils.fileutils import (
  get_temp_dir,
  check_file_path,
  read_json_data)
from igf_airflow.utils.generic_airflow_utils import (
  get_project_igf_id_for_analysis,
  parse_analysis_design_and_get_metadata,
  fetch_analysis_design)
from igf_nextflow.nextflow_utils.nextflow_input_formatter import (
  prepare_input_for_multiple_nfcore_pipeline)

log = logging.getLogger(__name__)

## CONF
SLACK_CONF = \
  Variable.get(
      'analysis_slack_conf',
      default_var=None)
MS_TEAMS_CONF = \
  Variable.get(
      'analysis_ms_teams_conf', default_var=None)
DATABASE_CONFIG_FILE = \
  Variable.get('database_config_file', default_var=None)
HPC_BASE_RAW_DATA_PATH = \
  Variable.get('hpc_base_raw_data_path', default_var=None)

## TASK
@task(
  task_id="prepare_nextflow_analysis_scripts",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G',
  multiple_outputs=False)
def prepare_nfcore_analysis_scripts(
      design_dict: dict,
      work_dir: str,
      script_template: str,
      config_template: str,
      nfcore_pipeline_name: str) -> dict:
  try:
    design_file = \
      design_dict.get('analysis_design')
    with open(design_file, 'r') as fp:
      input_design_yaml = fp.read()
    sample_metadata, analysis_metadata = \
      parse_analysis_design_and_get_metadata(
        input_design_yaml=input_design_yaml)
    if sample_metadata is None or \
       analysis_metadata is None:
        raise KeyError("Missing sample or analysis metadata")
    work_dir, runner_file = \
        prepare_input_for_multiple_nfcore_pipeline(
          runner_template_file=script_template,
          config_template_file=config_template,
          project_name=project_igf_id,
          hpc_data_dir=HPC_BASE_RAW_DATA_PATH,
          dbconf_file=DATABASE_CONFIG_FILE,
          sample_metadata=sample_metadata,
          analysis_metadata=analysis_metadata,
          nfcore_pipeline_name=nfcore_pipeline_name)
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)