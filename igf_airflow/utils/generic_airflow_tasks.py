import os
import shutil
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
  calculate_md5sum_for_analysis_dir,
  collect_analysis_dir,
  send_email_via_smtp,
  copy_analysis_to_globus_dir,
  fetch_analysis_yaml_and_dump_to_a_file)
from igf_data.utils.fileutils import (
    get_temp_dir,
    check_file_path,
    read_json_data)


log = logging.getLogger(__name__)

## GENERIC
SLACK_CONF = Variable.get('analysis_slack_conf',default_var=None)
MS_TEAMS_CONF = Variable.get('analysis_ms_teams_conf',default_var=None)
DATABASE_CONFIG_FILE = Variable.get('database_config_file', default_var=None)
HPC_BASE_RAW_DATA_PATH = Variable.get('hpc_base_raw_data_path', default_var=None)
## GLOBUS
GLOBUS_ROOT_DIR = Variable.get("globus_root_dir", default_var=None)
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
      email_user_key: str = 'username',
      analysis_email_template: str = ANALYSES_EMAIL_CONFIG) -> None:
  """
  An Airflow task for sending email to registered users for updating analysis pipeline status

  Parameters:
  send_email (bool): A toggle for sending email to primary user if "True" (default) or fall back to default user if "False"
  email_user_key (str): Key for the default user as mentioned in the email config file, default is 'username'
  analysis_email_template (str): A template for email, default is ANALYSES_EMAIL_CONFIG
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
        template_path=analysis_email_template,
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


## TASK
@task(
	task_id="calculate_md5sum_for_result_work_dir",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_8G',
  multiple_outputs=False)
def calculate_md5sum_for_main_work_dir(main_work_dir: str) -> str:
  try:
    md5_sum_file = \
      calculate_md5sum_for_analysis_dir(
        dir_path=main_work_dir)
    return main_work_dir
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)



## TASK
@task(
  task_id="copy_data_to_globus",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def copy_data_to_globus(analysis_dir_dict: dict) -> None:
  try:
    analysis_dir = analysis_dir_dict.get('target_dir_path')
    date_tag = analysis_dir_dict.get('date_tag')
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
    ## pipeline_name is context['task'].dag_id
    pipeline_name = context['task'].dag_id
    target_dir_path = \
      copy_analysis_to_globus_dir(
        globus_root_dir=GLOBUS_ROOT_DIR,
        dbconfig_file=DATABASE_CONFIG_FILE,
        analysis_id=analysis_id,
        analysis_dir=analysis_dir,
        date_tag=date_tag)
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


## TASK
@task(
  task_id="fetch_analysis_design",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def fetch_analysis_design_from_db() -> dict:
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
    ## pipeline_name is context['task'].dag_id
    pipeline_name = context['task'].dag_id
    ## get analysis design file
    temp_yaml_file = \
      fetch_analysis_yaml_and_dump_to_a_file(
        analysis_id=analysis_id,
        pipeline_name=pipeline_name,
        dbconfig_file=DATABASE_CONFIG_FILE)
    return {'analysis_design': temp_yaml_file}
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


## TASK
@task(
  task_id="create_main_work_dir",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G',
  multiple_outputs=False)
def create_main_work_dir(task_tag: str) -> str:
  try:
    main_work_dir = \
      get_temp_dir(
        use_ephemeral_space=True) ## get base dir
    main_work_dir = \
      os.path.join(
        main_work_dir,
        task_tag) ## add a custom name
    os.makedirs(
      main_work_dir,
      exist_ok=True) ## create path if its not present
    return main_work_dir
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


## TASK
@task(
  task_id="load_analysis_results_to_db",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def load_analysis_results_to_db(
      main_work_dir: str) -> str:
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
    ## load data to db
    target_dir_path, project_igf_id, date_tag = \
    collect_analysis_dir(
      analysis_id=analysis_id,
      dag_name=context['task'].dag_id,
      dir_path=main_work_dir,
      db_config_file=DATABASE_CONFIG_FILE,
      hpc_base_path=HPC_BASE_RAW_DATA_PATH)
    return {'target_dir_path': target_dir_path, 'date_tag': date_tag}
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


## TASK
@task(
  task_id="move_per_sample_analysis_to_main_work_dir",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def move_per_sample_analysis_to_main_work_dir(
      work_dir: str,
      analysis_output: dict) -> dict:
  try:
    check_file_path(work_dir)
    sample_id = analysis_output.get("sample_id")
    output_dir = analysis_output.get("output_dir")
    target_analysis_dir = \
      os.path.join(
        work_dir,
        os.path.basename(output_dir))
    ## not safe to overwrite existing dir
    if os.path.exists(target_analysis_dir):
      raise IOError(
        f"""Output path for sample {sample_id}) already present. \
          Path: {target_analysis_dir}. \
          CLEAN UP and RESTART !!!""")
    shutil.move(
      output_dir,
      work_dir)
    output_dict = {
      "sample_id": sample_id,
      "output": target_analysis_dir}
    return output_dict
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


## TASK: collect all analysis outputs
@task(
  task_id="collect_all_analysis",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def collect_all_analysis(
      analysis_output_list: list) -> list:
  try:
    return analysis_output_list
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)