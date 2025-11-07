import logging
from airflow.decorators import task
from airflow.models import Variable
from datetime import timedelta
from airflow.operators.python import get_current_context
from igf_airflow.utils.dag20_portal_metadata_utils import (
  _parse_default_user_email_from_email_config)
from igf_airflow.utils.generic_airflow_utils import (
  send_airflow_failed_logs_to_channels)
from igf_data.process.seqrun_processing.find_and_process_new_project_data_from_portal_db import (
  Find_and_register_new_project_data_from_portal_db)

log = logging.getLogger(__name__)

MS_TEAMS_CONF = \
  Variable.get('ms_teams_conf', default_var=None)
IGF_PORTAL_CONF = \
  Variable.get('igf_portal_conf', default_var=None)
EMAIL_TEMPLATE = \
  Variable.get('user_account_creation_template', default_var=None)
EMAIL_CONF = \
  Variable.get("email_config", default_var=None)
DATABASE_CONFIG_FILE = \
  Variable.get('database_config_file', default_var=None)

## TASK - find raw metadata id in datrun.conf
@task(
    task_id="find_raw_metadata_id",
    retry_delay=timedelta(minutes=5),
    retries=4,
    queue='hpc_4G',
    multiple_outputs=False)
def find_raw_metadata_id(
    raw_metadata_id_tag: str = "raw_metadata_id",
    dag_run_key: str = "dag_run") \
      -> int:
  try:
    ### dag_run.conf should have raw_analysis_id
    context = get_current_context()
    dag_run = context.get(dag_run_key)
    raw_metadata_id = None
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get(raw_metadata_id_tag) is not None:
      raw_metadata_id = \
        dag_run.conf.get(raw_metadata_id_tag)
    if raw_metadata_id is None:
      raise ValueError(
        'raw_metadata_id not found in dag_run.conf')
    return raw_metadata_id
  except Exception as e:
    message = \
      f"Failed to get raw_metadata_id, error: {e}"
    log.error(message)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(message))
    raise ValueError(message)


##TASK
@task(
  task_id="register_metadata_from_portal",
  retries=1,
  queue='hpc_4G',
  multiple_outputs=False)
def register_metadata_from_portal(raw_metadata_id: int):
  try:
    default_project_user_email = \
      _parse_default_user_email_from_email_config(EMAIL_CONF)
    fa = \
      Find_and_register_new_project_data_from_portal_db(
        portal_db_conf_file=IGF_PORTAL_CONF,
        dbconfig=DATABASE_CONFIG_FILE,
        user_account_template=EMAIL_TEMPLATE,
        default_user_email=default_project_user_email,
        raw_metadata_id=raw_metadata_id,
        log_slack=False,
        check_hpc_user=False,
        hpc_user=None,
        hpc_address=None,
        ldap_server=None,
        setup_irods=False,
        notify_user=True,
        email_config_json=EMAIL_CONF)
    fa.process_project_data_and_account()
  except Exception as e:
    message = \
      f"Failed to fetch raw_analysis_metadata, error: {e}"
    log.error(message)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(message))
    raise ValueError(message)