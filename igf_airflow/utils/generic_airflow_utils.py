import os
import logging
from datetime import timedelta
from igf_data.utils.fileutils import (
    get_temp_dir,
    check_file_path,
    read_json_data)
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from igf_airflow.logging.upload_log_msg import send_log_to_channels
from igf_airflow.utils.dag22_bclconvert_demult_utils import (
    send_email_via_smtp)
from igf_airflow.utils.dag26_snakemake_rnaseq_utils import (
    _create_output_from_jinja_template,
    generate_email_text_for_analysis)

log = logging.getLogger(__name__)

DATABASE_CONFIG_FILE = Variable.get('database_config_file', default_var=None)

def send_airflow_failed_logs_to_channels(
    slack_conf: str,
    ms_teams_conf: str,
    message_prefix: str) -> None:
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


def send_airflow_pipeline_logs_to_channels(
    slack_conf: str,
    ms_teams_conf: str,
    message_prefix: str) -> None:
  try:
    context = get_current_context()
    log_file_path = [
      os.environ.get('AIRFLOW__LOGGING__BASE_LOG_FOLDER'),
      f"dag_id={context['ti'].dag_id}",
      f"run_id={context['ti'].run_id}",
      f"task_id={context['ti'].task_id}",
      f"attempt={context['ti'].try_number}.log"]
    message = \
      f"Msg: {message_prefix}, Log: {os.path.join(*log_file_path)}"
    send_log_to_channels(
      slack_conf=slack_conf,
      ms_teams_conf=ms_teams_conf,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      project_id=None,
      comment=message,
      reaction='pass')
  except Exception as e:
    log.error(e)


def format_and_send_email_to_user(
      email_template: str,
      email_config_file: str,
      analysis_id: int,
      database_config_file: str,
      email_user_key: str = 'username',
      send_email: bool = True) \
        -> None:
  try:
    check_file_path(email_template)
    check_file_path(email_config_file)
    check_file_path(database_config_file)
    ## get default user from email config
    email_config = \
      read_json_data(email_config_file)
    if isinstance(email_config, list):
      email_config = email_config[0]
    default_email_user = \
      email_config.get(email_user_key)
    if default_email_user is None:
      raise KeyError(
        f"Missing default user info in email config file {email_config_file}")
    ## generate email text for analysis
    email_text_file, receivers = \
      generate_email_text_for_analysis(
        analysis_id=analysis_id,
        template_path=email_template,
        dbconfig_file=DATABASE_CONFIG_FILE,
        default_email_user=default_email_user,
        send_email_to_user=send_email)
    ## send email to user
    send_email_via_smtp(
      sender=default_email_user,
      receivers=receivers,
      email_config_json=email_config_file,
      email_text_file=email_text_file)
  except Exception as e:
    raise ValueError(f"Failed to send email, error: {e}")


def format_and_send_generic_email_to_user(
      user_name: str,
      user_email: str,
      email_template: str,
      email_config_file: str,
      email_user_key: str = 'username',
      send_email: bool = True,
      email_data: dict = {}) \
        -> None:
  try:
    check_file_path(email_template)
    check_file_path(email_config_file)
    ## get default user from email config
    email_config = \
      read_json_data(email_config_file)
    if isinstance(email_config, list):
      email_config = email_config[0]
    default_email_user = \
      email_config.get(email_user_key)
    if default_email_user is None:
      raise KeyError(
        f"Missing default user info in email config file {email_config_file}")
    ## generate email text
    temp_dir = \
      get_temp_dir(use_ephemeral_space=True)
    output_file = \
      os.path.join(temp_dir, 'email.txt')
    email_template_data = \
      dict(
        user_email=user_email,
        defaultUser=default_email_user,
        user_name=user_name,
        send_email_to_user=send_email)
    if len(email_data) > 0:
      email_template_data.\
        update(**email_data)
    _create_output_from_jinja_template(
      template_file=email_template,
      output_file=output_file,
      autoescape_list=['xml', 'html'],
      data=email_template_data)
    ## send email to user
    send_email_via_smtp(
      sender=default_email_user,
      receivers=[user_email, default_email_user],
      email_config_json=email_config_file,
      email_text_file=output_file)
  except Exception as e:
    raise ValueError(
      f"Failed to send email, error: {e}")