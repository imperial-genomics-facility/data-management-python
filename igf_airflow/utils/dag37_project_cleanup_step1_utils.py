import os
import json
import logging
import pandas as pd
from datetime import timedelta
from airflow.models import Variable
from igf_data.utils.fileutils import (
  get_temp_dir,
  check_file_path)
from igf_portal.metadata_utils import _gzip_json_file
from igf_portal.api_utils import upload_files_to_portal
from airflow.operators.python import get_current_context
from airflow.decorators import task
from igf_airflow.logging.upload_log_msg import send_log_to_channels
from igf_airflow.utils.dag22_bclconvert_demult_utils import (
  _create_output_from_jinja_template)
from igf_airflow.utils.generic_airflow_utils import send_airflow_failed_logs_to_channels
from igf_data.utils.projectutils import find_projects_for_cleanup

log = logging.getLogger(__name__)

SLACK_CONF = \
  Variable.get('analysis_slack_conf',default_var=None)
MS_TEAMS_CONF = \
  Variable.get('analysis_ms_teams_conf',default_var=None)
HPC_SSH_KEY_FILE = \
  Variable.get('hpc_ssh_key_file', default_var=None)
DATABASE_CONFIG_FILE = \
  Variable.get('database_config_file', default_var=None)
IGF_PORTAL_CONF = \
  Variable.get('igf_portal_conf', default_var=None)
PORTAL_ADD_PROJECT_CLEANUP_DATA_URI = \
  Variable.get('portal_add_project_cleanup_data_uri', default_var=None)

## TASK
@task.branch(
  task_id="find_projects_for_cleanup",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def find_projects_for_cleanup(
      next_task: str,
      no_task: str,
      xcom_key: str,
      cutoff_weeks: int = 16) -> list:
  try:
    task_list = list()
    ## get_cleanup_list
    cleanup_list = \
      find_projects_for_cleanup(
        dbconfig_file=DATABASE_CONFIG_FILE,
        cutoff_weeks=cutoff_weeks)
    if len(cleanup_list) > 0:
      temp_dir = \
        get_temp_dir(use_ephemeral_space=True)
      json_file = \
        os.path.josn(
          temp_dir,
          'project_cleanup_data.json')
      with open(json_file, 'w') as fp:
        json.dump(cleanup_list, fp)
      ## add json file path to xcom
      context = get_current_context()
      ti = context.get('ti')
      ti.xcom_push(
        key=xcom_key,
        value=json_file)
      task_list.append(next_task)
    ## no task to do
    if len(task_list) == 0:
      task_list.append(no_task)
    return task_list
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


## CHANGE ME: Once EmptyOperator has a decorator then remove this
@task(
    task_id="upload_project_cleanup_data_to_portal",
    retry_delay=timedelta(minutes=5),
    retries=4,
    queue='hpc_4G')
def upload_project_cleanup_data_to_portal(
      xcom_key: str,
      xcom_task: str) -> None:
  try:
    context = get_current_context()
    ti = context.get('ti')
    json_file = \
      ti.xcom_pull(task_ids=xcom_task, key=xcom_key)
    if json_file is None:
      raise ValueError(
        f"JSON file not found for key {xcom_key} and task {xcom_task}")
    check_file_path(json_file)
    gzip_json_dump_file = \
      _gzip_json_file(json_file)
    upload_files_to_portal(
      portal_config_file=IGF_PORTAL_CONF,
      file_path=gzip_json_dump_file,
      url_suffix=PORTAL_ADD_PROJECT_CLEANUP_DATA_URI)
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)