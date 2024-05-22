import os
import json
import logging
import pandas as pd
from datetime import timedelta
from airflow.models import Variable
from igf_data.utils.fileutils import (
  get_temp_dir,
  read_json_data,
  check_file_path)
from igf_portal.api_utils import get_data_from_portal
from airflow.operators.python import get_current_context
from airflow.decorators import task
from igf_data.utils.projectutils import mark_project_and_list_files_for_cleanup
from igf_airflow.utils.generic_airflow_utils import (
    send_airflow_failed_logs_to_channels,
    send_airflow_pipeline_logs_to_channels,
    format_and_send_generic_email_to_user)

log = logging.getLogger(__name__)

## CHANNELS
SLACK_CONF = \
  Variable.get('analysis_slack_conf',default_var=None)
MS_TEAMS_CONF = \
  Variable.get('analysis_ms_teams_conf',default_var=None)

## PORTAL
IGF_PORTAL_CONF = \
  Variable.get('igf_portal_conf', default_var=None)
PROJECT_CLEANUP_MARK_DB_CLEANUP_FINISHED_URI = \
  Variable.get('portal_project_cleanup_mark_db_cleanup_finished_uri', default_var=None)

## DB
DATABASE_CONFIG_FILE = \
  Variable.get('database_config_file', default_var=None)

## EMAIL CONFIG
EMAIL_CONFIG = \
  Variable.get("email_config", default_var=None)
EMAIL_TEMPLATE = \
  Variable.get("project_cleanup_finished_email_notification_template", default_var=None)

@task(
  task_id="cleanup_old_project_in_db",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def cleanup_old_project_in_db(
      project_cleanup_data_file: str) -> None:
  try:
    check_file_path(project_cleanup_data_file)
    json_data = read_json_data(project_cleanup_data_file)
    if isinstance(json_data, list):
      json_data = json_data[0]
    ## TO DO: check project after getting json dump
    projects = json_data.get("projects")
    if isinstance(projects, str):
      projects = json.loads(projects)
    if not isinstance(projects, list) or \
       len(projects)==0:
      raise ValueError(f"No projects found for cleanup in {projects}")
    ## cleanup projects
    deletion_dir = \
      cleanup_igf_projects_on_db(
        projects=projects,
        dbconfig_file=DATABASE_CONFIG_FILE)
    message_text = \
      f"Marked projects DELETED in DB: {','.join(projects)}. Deletion list dir: {deletion_dir}."
    send_airflow_pipeline_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=message_text)
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)

def cleanup_igf_projects_on_db(
      projects: list,
      dbconfig_file: str) -> str:
  try:
    temp_dir = \
      get_temp_dir(use_ephemeral_space=True)
    for project_igf_id in projects:
       mark_project_and_list_files_for_cleanup(
        project_igf_id=project_igf_id,
        dbconfig_file=dbconfig_file,
        outout_dir=temp_dir)
    return temp_dir
  except Exception as e:
    raise ValueError(
      f"Failed to mark projects as deleted on db. Error: {e}")

@task(
  task_id="mark_project_deleted_on_portal",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def mark_project_deleted_on_portal() -> None:
  try:
    ## dag_run.conf should have project_cleanup_id
    context = get_current_context()
    dag_run = context.get('dag_run')
    project_cleanup_id = None
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('project_cleanup_id') is not None:
      project_cleanup_id = \
        dag_run.conf.get('project_cleanup_id')
    if project_cleanup_id is None:
      raise ValueError(
        'project_cleanup_id not found in dag_run.conf')
    _ = \
      get_data_from_portal(
        portal_config_file=IGF_PORTAL_CONF,
        url_suffix=f'{PROJECT_CLEANUP_MARK_DB_CLEANUP_FINISHED_URI}/{project_cleanup_id}',
        request_mode='post')
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)

@task(
  task_id="notify_user_about_project_cleanup_finished",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def notify_user_about_project_cleanup_finished(
      project_cleanup_data_file: str,
      send_email: bool = False,
      email_user_key: str = 'username') -> None:
  try:
    check_file_path(project_cleanup_data_file)
    json_data = read_json_data(project_cleanup_data_file)
    if isinstance(json_data, list):
      json_data = json_data[0]
    user_name = json_data.get("user_name")
    user_email = json_data.get("user_email")
    ## TO DO: check project after getting json dump
    projects = json_data.get("projects")
    deletion_date = json_data.get("deletion_date")
    ## get default user from email config
    format_and_send_generic_email_to_user(
      user_name=user_name,
      user_email=user_email,
      email_template=EMAIL_TEMPLATE,
      email_config_file=EMAIL_CONFIG,
      email_user_key=email_user_key,
      send_email=send_email,
      email_data=dict(
        projectLists=projects,
        deletionDate=deletion_date))
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)