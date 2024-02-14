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
from airflow.operators.python import get_current_context
from airflow.decorators import task
from igf_airflow.logging.upload_log_msg import send_log_to_channels
from igf_airflow.utils.dag22_bclconvert_demult_utils import (
  _create_output_from_jinja_template)
from igf_airflow.utils.generic_airflow_utils import (
    send_airflow_failed_logs_to_channels,
    format_and_send_email_to_user)


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
  task_id="send_email_to_user",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def send_email_to_user(
      send_email: bool = True,
      email_user_key: str = 'username') -> None:
  try:
    ## dag_run.conf should have analysis_id
    context = get_current_context()
    dag_run = context.get('dag_run')
    analysis_id = None
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('analysis_id') is not None:
      analysis_id = \
        dag_run.conf.get('analysis_id')
      if analysis_id is None:
        raise ValueError(
          'analysis_id not found in dag_run.conf')
      ## get default user from email config
      format_and_send_email_to_user(
        email_template=EMAIL_TEMPLATE,
        email_config_file=EMAIL_CONFIG,
        analysis_id=analysis_id,
        database_config_file=DATABASE_CONFIG_FILE,
        email_user_key=email_user_key,
        send_email=send_email)
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)