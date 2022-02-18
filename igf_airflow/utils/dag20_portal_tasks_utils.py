import os, logging
from airflow.models import Variable
from igf_data.utils.fileutils import get_temp_dir, remove_dir
from igf_airflow.logging.upload_log_msg import send_log_to_channels
from igf_portal.metadata_utils import get_db_data_and_create_json_dump

DATABASE_CONFIG_FILE = \
  Variable.get('database_config_file', default_var=None)
SLACK_CONF = \
  Variable.get('slack_conf', default_var=None)
MS_TEAMS_CONF = \
  Variable.get('ms_teams_conf', default_var=None)


def get_metadata_dump_from_pipeline_db_func(**context):
  try:
    ti = context.get('ti')
    xcom_key = \
      context['params'].get('json_dump_xcom_key')
    temp_dir = \
      get_temp_dir(use_ephemeral_space=True)
    temp_metadata_dump_json = \
      os.path.join(temp_dir, 'metadata_dump.json')
    get_db_data_and_create_json_dump(
        dbconfig_json=DATABASE_CONFIG_FILE,
        output_json_path=temp_metadata_dump_json)
    ti.xcom_push(
      key=xcom_key,
      value=temp_metadata_dump_json)
  except Exception as e:
    logging.error(e)
    message = \
      'failed metadata dump, error: {0}'.\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise
