import os
import re
import shutil
import logging
import subprocess
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
SLACK_CONF = \
  Variable.get(
      'analysis_slack_conf',
      default_var=None)
MS_TEAMS_CONF = \
  Variable.get(
      'analysis_ms_teams_conf', default_var=None)
DATABASE_CONFIG_FILE = \
  Variable.get('database_config_file', default_var=None)
CURIOSEEKER_TEMPLATE = \
  Variable.get('curioseeker_template', default_var=None)
CURIOSEEKER_NF_CONFIG_TEMPLATE = \
  Variable.get('curioseeker_nf_config_template', default_var=None)


def merge_fastqs_for_analysis(analysis_entry: dict) -> dict:
  try:
    sample_metadata = \
      analysis_entry.get("sample_metadata")
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


def fetch_and_merge_fastqs_for_samples(
      sample_dict: dict,
      db_config_file: str,
      fastq_file_input_column: str = 'file_path') \
        -> dict:
  try:
    fastq1_pattern = \
      re.compile(r'\S+_R1_001.fastq.gz')
    output_dict = dict()
    for sample_id, sample_info in sample_dict.items():
      ## get fastq files for all samples
      fastq_list = \
        get_fastq_and_run_for_samples(
          dbconfig_file=db_config_file,
          sample_igf_id_list=[sample_id,])
      if len(fastq_list) == 0:
        raise ValueError(
          "No fastq file found for samples")
      ## get fastq dirs
      fastq_df = pd.DataFrame(fastq_list)
      fastq_files = \
        fastq_df[fastq_file_input_column].\
          values.tolist()
      ## find R1 files
      r1_fastq_list = [
        f for f in fastq_files
          if re.search(fastq1_pattern, f)]
      ## create R2 list
      r2_fastq_list = [
        f.replace("_R1_", "_R2_")
          for f in r1_fastq_list]
      ## check if R1 file is present for sample
      if len(r1_fastq_list) == 0:
        raise ValueError(
          f"Missing R1 fastq for sample {sample_id}")
      ## check if merged is needed
      merged_r1_fastq, merged_r2_fastq = \
        merge_list_of_r1_and_r2_files(
          r1_list=r1_fastq_list,
          r2_list=r2_fastq_list)
      sample_info.update({
        "R1": merged_r1_fastq,
        "R2": merged_r2_fastq})
      output_dict.update({
        sample_id: sample_info})
    return output_dict
  except Exception as e:
    raise ValueError(
      f"Failed to merge fastqs, error: {e}")


def merge_list_of_r1_and_r2_files(
      r1_list: list,
      r2_list: list,
      merged_r1_filename: str = "merged_R1_001.fastq.gz",
      merged_r2_filename: str = "merged_R2_001.fastq.gz")\
        -> Tuple[str, str]:
  try:
    merged_r1_fastq = None
    merged_r2_fastq = None
    if len(r1_list)==0 or \
       len(r2_list)==0:
      raise ValueError(
        f"Missing R1 or R2")
    if len(r1_list) != len(r2_list):
      raise ValueError(
        f"R1 or R2 list are not matching")
    if len(r1_list)==1:
      merged_r1_fastq = r1_list[0]
      merged_r2_fastq = r2_list[0]
    else:
      ## merge list of r1 and r2 files
      work_dir = get_temp_dir()
      merged_r1_fastq = \
        os.path.join(
          work_dir,
          merged_r1_filename)
      merged_r2_fastq = \
        os.path.join(
          work_dir,
          merged_r2_filename)
      cmd_r1 = \
        f"cat {' '.join(r1_list)} > {merged_r1_fastq}"
      cmd_r2 = \
        f"cat {' '.join(r2_list)} > {merged_r2_fastq}"
      subprocess.\
        check_call(cmd_r1, shell=True)
      subprocess.\
        check_call(cmd_r2, shell=True)
    ## check if merged files are found
    if merged_r1_fastq is None or \
       merged_r2_fastq is None:
      raise ValueError(
        f"No merged file found")
    check_file_path(merged_r1_fastq)
    check_file_path(merged_r2_fastq)
    return merged_r1_fastq, merged_r2_fastq
  except Exception as e:
    raise ValueError(
      f"Failed to merge fastqs, error: {e}")