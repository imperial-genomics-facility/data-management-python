import os
import logging
from airflow.operators.python import get_current_context
from igf_airflow.logging.upload_log_msg import send_log_to_channels

log = logging.getLogger(__name__)

def send_airflow_failed_logs_to_channels(
    slack_conf: str,
    ms_teams_conf: str,
    message_prefix: str = 'Error') -> None:
  try:
    context = get_current_context()
    log_file_path = [
      os.environ.get('AIRFLOW__LOGGING__BASE_LOG_FOLDER'),
      f"dag_id={context['ti'].dag_id}",
      f"run_id={context['ti'].run_id}",
      f"task_id={context['ti'].task_id}",
      f"attempt={context['ti'].try_number}.log"]
    message = \
      f"Error: {message_prefix}, Log: {os.path.join(*log_file_path)}"
    send_log_to_channels(
      slack_conf=slack_conf,
      ms_teams_conf=ms_teams_conf,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      project_id=None,
      comment=message,
      reaction='fail')
  except Exception as e:
    log.error(e)