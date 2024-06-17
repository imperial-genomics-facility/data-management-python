import os
import logging
from datetime import timedelta
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from igf_airflow.utils.generic_airflow_utils import (
  check_and_seed_analysis_pipeline,
  send_airflow_pipeline_logs_to_channels,
  send_airflow_failed_logs_to_channels,
  generate_email_text_for_analysis,
  send_email_via_smtp)
from igf_data.utils.fileutils import (
    get_temp_dir,
    check_file_path,
    read_json_data)


log = logging.getLogger(__name__)

## GENERIC
SLACK_CONF = Variable.get('analysis_slack_conf',default_var=None)
MS_TEAMS_CONF = Variable.get('analysis_ms_teams_conf',default_var=None)
DATABASE_CONFIG_FILE = Variable.get('database_config_file', default_var=None)

## EMAIL CONFIG
EMAIL_CONFIG = Variable.get("email_config", default_var=None)
ANALYSES_EMAIL_CONFIG = Variable.get("analysis_email_template", default_var=None)

## TASK
@task.branch(
  task_id="mark_analysis_running",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def mark_analysis_running(
    next_task: str,
    last_task: str,
    seed_table: str = 'analysis',
    new_status: str = 'RUNNING',
    no_change_status: list = ['RUNNING', 'FAILED', 'FINISHED', 'UNKNOWN']) -> list:
  """
  A generic Airflow branch task for marking pipeline as running on the pipeseed table

  Parameters:
  next_task (str): Next task name
  last_task (str): Second task name to fall back
  seed_table (str): Pipeline_seed table name, default is 'analysis',
  new_status (str): New status, default is 'RUNNING',
  no_change_status (list): A list of status to check and skip current operation, default ['RUNNING', 'FAILED', 'FINISHED', 'UNKNOWN']
  
  Returns:
  A list of tasks
  """
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
      raise ValueError('analysis_id not found in dag_run.conf')
    ## pipeline_name is context['task'].dag_id
    pipeline_name = context['task'].dag_id
    ## change seed status
    seed_status = \
      check_and_seed_analysis_pipeline(
        analysis_id=analysis_id,
        pipeline_name=pipeline_name,
        dbconf_json_path=DATABASE_CONFIG_FILE,
        new_status=new_status,
        seed_table=seed_table,
        no_change_status=no_change_status)
    ## set next tasks
    task_list = list()
    if seed_status:
      task_list.append(next_task)
    else:
      task_list.append(last_task)
      message_text = f"No task for analysis: {analysis_id}, pipeline: {pipeline_name}"
      send_airflow_pipeline_logs_to_channels(
        slack_conf=SLACK_CONF,
        ms_teams_conf=MS_TEAMS_CONF,
        message_prefix=message_text)
    return task_list
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


## TASK
@task(
  task_id="mark_analysis_finished",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def mark_analysis_finished(
      seed_table: str = 'analysis',
      new_status: str = 'FINISHED',
      no_change_status: list = ('SEEDED', )) -> None:
  """
  A generic Airflow task for marking pipeline as finished on the pipeseed table

  Parameters:
  seed_table (str): Pipeline_seed table name, default is 'analysis',
  new_status (str): New status, default is 'FINISHED',
  no_change_status (list): A list of status to check and skip current operation, default ['SEEDED']
  
  Returns:
  None
  """
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
      raise ValueError('analysis_id not found in dag_run.conf')
    ## pipeline_name is context['task'].dag_id
    pipeline_name = context['task'].dag_id
    ## change seed status
    seed_status = \
      check_and_seed_analysis_pipeline(
        analysis_id=analysis_id,
        pipeline_name=pipeline_name,
        dbconf_json_path=DATABASE_CONFIG_FILE,
        new_status=new_status,
        seed_table=seed_table,
        no_change_status=no_change_status)
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


## TASK
@task(
  task_id="mark_analysis_failed",
  retry_delay=timedelta(minutes=5),
  retries=4,
  trigger_rule='all_failed',
  queue='hpc_4G')
def mark_analysis_failed(
      seed_table: str = 'analysis',
      new_status: str = 'FAILED',
      no_change_status: list = ('SEEDED', 'FINISHED')) -> None:
  """
  A generic Airflow task for marking pipeline as failed on the pipeseed table

  Parameters:
  seed_table (str): Pipeline_seed table name, default is 'analysis',
  new_status (str): New status, default is 'FAILED',
  no_change_status (list): A list of status to check and skip current operation, default ['SEEDED', 'FINISHED']
  
  Returns:
  None
  """
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
      raise ValueError('analysis_id not found in dag_run.conf')
    ## pipeline_name is context['task'].dag_id
    pipeline_name = context['task'].dag_id
    ## change seed status
    seed_status = \
      check_and_seed_analysis_pipeline(
        analysis_id=analysis_id,
        pipeline_name=pipeline_name,
        dbconf_json_path=DATABASE_CONFIG_FILE,
        new_status=new_status,
        seed_table=seed_table,
        no_change_status=no_change_status)
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


## TASK
@task(
  task_id="send_email_to_user",
  retry_delay=timedelta(minutes=5),
  retries=4,
  trigger_rule="none_failed_min_one_success",
  queue='hpc_4G')
def send_email_to_user(
      send_email: bool = True,
      email_user_key: str = 'username') -> None:
  """
  An Airflow task for sending email to registered users for updating analysis pipeline status

  Parameters:
  send_email (bool): A toggle for sending email to primary user if "True" (default) or fall back to default user if "False"
  email_user_key (str): Key for the default user as mentioned in the email config file, default is 'username'
  """
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
    email_config = \
      read_json_data(EMAIL_CONFIG)
    if isinstance(email_config, list):
      email_config = email_config[0]
    default_email_user = \
      email_config.get(email_user_key)
    if default_email_user is None:
      raise KeyError(
        f"Missing default user info in email config file {EMAIL_CONFIG}")
    ## generate email text for analysis
    email_text_file, receivers = \
      generate_email_text_for_analysis(
        analysis_id=analysis_id,
        template_path=ANALYSES_EMAIL_CONFIG,
        dbconfig_file=DATABASE_CONFIG_FILE,
        default_email_user=default_email_user,
        send_email_to_user=send_email)
    ## send email to user
    send_email_via_smtp(
      sender=default_email_user,
      receivers=receivers,
      email_config_json=EMAIL_CONFIG,
      email_text_file=email_text_file)
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)

## TASK
## CHANGE ME: FIX ME, we don't need this anymore
@task(
  task_id="no_work",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def no_work() -> None:
  try:
    pass
  except Exception as e:
    raise ValueError(e)