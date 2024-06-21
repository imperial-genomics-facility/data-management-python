import os
import shutil
import logging
import pandas as pd
from datetime import timedelta
from airflow.models import Variable
from igf_data.utils.bashutils import bash_script_wrapper
from igf_data.utils.analysis_fastq_fetch_utils import get_fastq_and_run_for_samples
from igf_data.utils.jupyter_nbconvert_wrapper import Notebook_runner
from typing import (
    Tuple,
    Optional)
from igf_data.utils.fileutils import (
  check_file_path,
  copy_local_file,
  get_temp_dir,
  get_date_stamp)
from igf_airflow.logging.upload_log_msg import send_log_to_channels
from igf_airflow.utils.dag22_bclconvert_demult_utils import (
  _create_output_from_jinja_template)
from igf_airflow.utils.generic_airflow_utils import (
    get_project_igf_id_for_analysis,
    fetch_analysis_name_for_analysis_id,
    send_airflow_failed_logs_to_channels,
    collect_analysis_dir,
    parse_analysis_design_and_get_metadata
)
from airflow.operators.python import get_current_context
from airflow.decorators import task

log = logging.getLogger(__name__)

## CONF
SLACK_CONF = Variable.get('analysis_slack_conf',default_var=None)
MS_TEAMS_CONF = Variable.get('analysis_ms_teams_conf',default_var=None)
DATABASE_CONFIG_FILE = Variable.get('database_config_file', default_var=None)
SPACERANGER_COUNT_SCRIPT_TEMPLATE = \
  Variable.get("spaceranger_count_script_template", default_var=None)

## TASK
@task(
  task_id="get_spaceranger_analysis_group_list",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G',
  multiple_outputs=False)
def get_spaceranger_analysis_group_list(design_dict: dict) -> dict:
  try:
    design_file = design_dict.get('analysis_design')
    check_file_path(design_file)
    with open(design_file, 'r') as fp:
      input_design_yaml = fp.read()
      sample_metadata, analysis_metadata = \
        parse_analysis_design_and_get_metadata(
          input_design_yaml=input_design_yaml)
    if sample_metadata is None or \
       analysis_metadata is None:
      raise KeyError("Missing sample or analysis metadata")
    unique_sample_groups = list()
    for sample_name, sample_data in sample_metadata.items():
      unique_sample_groups.\
        append({
          "sample_metadata": {
            sample_name: sample_data},
          "analysis_metadata": analysis_metadata})
    if len(unique_sample_groups) == 0:
      raise ValueError("No sample group found")
    return unique_sample_groups
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


def prepare_spaceranger_count_run_dir_and_script_file(
      sample_metadata: dict,
      analysis_metadata: dict,
      db_config_file: str,
      run_script_template: str,
      spaceranger_config_key: str = "spaceranger_count_config") -> str:
  try:
    ## check if sample info is present
    sample_list = \
      list(sample_metadata.keys())
    if len(sample_list) == 0:
      raise ValueError(
        f"No sample id found in the sample metadata: {sample_metadata}")
    sample_id = sample_list[0]
    sample_info = sample_metadata.get(sample_id)
    if sample_info is None:
      raise ValueError(
        f"No sample info found for sample {sample_id}")
    ## check if input files are present
    check_file_path(db_config_file)
    check_file_path(run_script_template)
    ## get fastq files for all samples
    fastq_list = \
      get_fastq_and_run_for_samples(
        dbconfig_file=db_config_file,
        sample_igf_id_list=[sample_id,])
    if len(fastq_list) == 0:
      raise ValueError(
        "No fastq file found for samples")
    ## get fastqs
    df = pd.DataFrame(fastq_list)
    fastqs = \
      df['file_path'].values.tolist()
    fastqs = \
      ','.join(fastqs)
    # set parameters
    spaceranger_params = list()
    ## image parameters
    for param_key, param_val in sample_info.items():
      spaceranger_params.\
        append(f"--{param_key}={param_val}")
    if len(spaceranger_params) == 0:
      raise ValueError(
        f"No image param found for sample {sample_id}")
    spaceranger_count_config = \
      analysis_metadata.get(spaceranger_config_key)
    if spaceranger_count_config is None or \
       not isinstance(spaceranger_count_config, list) or \
       len(spaceranger_count_config) == 0:
      raise ValueError(
        f"spaceranger_count_config is not correct: {spaceranger_count_config}")
    ## add analysis config
    spaceranger_params.extend(
      spaceranger_count_config)
    spaceranger_params = \
      ' '.join(spaceranger_params)
    ## create run script from template
    work_dir = \
      get_temp_dir(use_ephemeral_space=True)
    script_file = \
      os.path.join(
        work_dir,
        os.path.basename(run_script_template))
    _create_output_from_jinja_template(
      template_file=run_script_template,
      output_file=script_file,
      autoescape_list=['xml',],
      data=dict(
        SPACERANGER_ID=str(sample_id),
        FASTQS=fastqs,
        SPACERANGER_PARAMS=spaceranger_params,
        WORKDIR=work_dir))
    return script_file
  except Exception as e:
    raise ValueError(
      f"Failed to create spaceranger count script and dir, error: {e}")




## TASK
@task(
  task_id="prepare_spaceranger_count_script",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def prepare_spaceranger_count_script(analysis_entry: dict) \
      -> dict:
  try:
    sample_metadata = \
      analysis_entry.get("sample_metadata")
    analysis_metadata = \
      analysis_entry.get("analysis_metadata")
    run_script_file = \
      prepare_spaceranger_count_run_dir_and_script_file(
        sample_metadata=sample_metadata,
        analysis_metadata=analysis_metadata,
        db_config_file=DATABASE_CONFIG_FILE,
        run_script_template=SPACERANGER_COUNT_SCRIPT_TEMPLATE)
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)