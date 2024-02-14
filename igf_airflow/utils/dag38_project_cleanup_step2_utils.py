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
from igf_portal.metadata_utils import _gzip_json_file
from airflow.operators.python import get_current_context
from airflow.decorators import task
from igf_airflow.logging.upload_log_msg import send_log_to_channels
from igf_airflow.utils.dag22_bclconvert_demult_utils import (
  _create_output_from_jinja_template)
from igf_airflow.utils.generic_airflow_utils import (
    send_airflow_failed_logs_to_channels,
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

## DB
DATABASE_CONFIG_FILE = Variable.get('database_config_file', default_var=None)

## EMAIL CONFIG
EMAIL_CONFIG = Variable.get("email_config", default_var=None)
EMAIL_TEMPLATE = Variable.get("project_cleanup_email_notification_template", default_var=None)

@task(
  task_id="notify_user_about_project_cleanup",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def notify_user_about_project_cleanup(
      project_cleanup_data_file: str,
      send_email: bool = True,
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