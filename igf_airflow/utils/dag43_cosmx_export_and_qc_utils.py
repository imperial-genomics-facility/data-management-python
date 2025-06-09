import os
import re
import shutil
import logging
import subprocess
import pandas as pd
from datetime import timedelta
from airflow.models import Variable 
from igf_data.utils.bashutils import bash_script_wrapper
from igf_data.utils.jupyter_nbconvert_wrapper import Notebook_runner
from typing import (
  Any,
  List,
  Dict,
  Tuple,
  Union,
  Optional)
from igf_data.utils.fileutils import (
  check_file_path,
  copy_local_file,
  get_temp_dir,
  get_date_stamp)
from igf_airflow.utils.dag22_bclconvert_demult_utils import (
  _create_output_from_jinja_template)
from igf_airflow.utils.generic_airflow_utils import (
  get_project_igf_id_for_analysis,
  fetch_analysis_name_for_analysis_id,
  send_airflow_failed_logs_to_channels,
  send_airflow_pipeline_logs_to_channels,
  get_per_sample_analysis_groups,
  collect_analysis_dir,
  parse_analysis_design_and_get_metadata)
from airflow.operators.python import get_current_context
from airflow.decorators import task

log = logging.getLogger(__name__)

## CONF
MS_TEAMS_CONF = \
  Variable.get(
      'analysis_ms_teams_conf', default_var=None)
DATABASE_CONFIG_FILE = \
  Variable.get('database_config_file', default_var=None)

## TASKS

@task(multiple_outputs=False)
def run_ftp_export_factory(design_file: str, work_dir: str) -> List[Dict[str, str]]:
  try:
    design_data = [{}]
    return design_data
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))
    raise ValueError(e)


@task(multiple_outputs=False)
def run_ftp_export(run_entry: Dict[str, str], work_dir: str) -> Dict[str, str]:
  try:
    exported_data = {}
    return exported_data
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))
    raise ValueError(e)


@task(multiple_outputs=False)
def extract_ftp_export(run_entry: Dict[str, str]) -> Dict[str, str]:
  try:
    extracted_data = {}
    return extracted_data
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))
    raise ValueError(e)


@task(multiple_outputs=False)
def collect_all_slides(run_entry_list: Union[List[Dict[str, str]], Any]) -> Optional[List[Dict[str, str]]]:
  try:
    slide_data = [{}]
    return slide_data
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))


@task(multiple_outputs=False)
def validate_export_md5(run_entry: Dict[str, str]) -> Dict[str, str]:
  try:
    validated_data = {}
    return validated_data
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))
    raise ValueError(e)


@task(multiple_outputs=False)
def generate_count_qc_report(run_entry: Dict[str, str]) -> Dict[str, str]:
  try:
    count_qc_data = {}
    return count_qc_data
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))
    raise ValueError(e)


@task(multiple_outputs=False)
def generate_fov_qc_report(run_entry: Dict[str, str]) -> Dict[str, str]:
  try:
    fov_qc_data = {}
    return fov_qc_data
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))
    raise ValueError(e)


@task(multiple_outputs=False)
def generate_db_data(qc_list: List[Dict[str, str]]) -> Dict[str, str]:
  try:
    db_data = {}
    return db_data
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))
    raise ValueError(e)


@task(multiple_outputs=False)
def copy_slide_data_to_globus(run_entry: Dict[str, str]) -> Dict[str, str]:
  try:
    globus_data = {}
    return globus_data
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))
    raise ValueError(e)


@task(multiple_outputs=False)
def register_db_data(run_entry: Dict[str, str]) -> Dict[str, str]:
  try:
    registered_data = {}
    return registered_data
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))
    raise ValueError(e)


@task(multiple_outputs=False)
def collect_qc_reports_and_upload_to_portal(run_entry_list: Union[List[Dict[str, str]], Any]) -> Optional[bool]:
  try:
    return True
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))