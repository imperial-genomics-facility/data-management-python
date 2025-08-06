import os
import re
import json
import yaml
import logging
from typing import (
    Tuple,
    Optional)
from igf_data.utils.fileutils import (
    get_temp_dir,
    copy_local_file,
    check_file_path,
    read_json_data,
    get_date_stamp_for_file_name)
from jinja2 import Template
from igf_data.utils.dbutils import read_dbconf_json
from airflow.operators.python import get_current_context
from igf_data.utils.bashutils import bash_script_wrapper
from igf_airflow.logging.upload_log_msg import send_log_to_channels
from igf_data.utils.analysis_fastq_fetch_utils import get_fastq_and_run_for_samples
from igf_airflow.utils.dag22_bclconvert_demult_utils import (
    _create_output_from_jinja_template,
    send_email_via_smtp)
from igf_data.igfdb.analysisadaptor import AnalysisAdaptor
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor

log = logging.getLogger(__name__)


def get_analysis_id_and_project_igf_id_from_airflow_dagrun_conf(
  database_config_file: str,
  analysis_id_key: str = "analysis_id",
  dag_run_key: str = "dag_run") -> Tuple[str, str]:
  """
  """
  try:
    ## step 1: get analysis id
    ### dag_run.conf should have analysis_id
    context = get_current_context()
    dag_run = context.get(dag_run_key)
    analysis_id = None
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get(analysis_id_key) is not None:
      analysis_id = \
        dag_run.conf.get(analysis_id_key)
    if analysis_id is None:
      raise ValueError(
        'analysis_id not found in dag_run.conf')
    ## step 2: get project id of analysis
    project_igf_id = \
      get_project_igf_id_for_analysis(
        analysis_id=analysis_id,
        dbconfig_file=database_config_file)
    if project_igf_id is None:
      raise ValueError(
        f"No valid project_igf_id found for analysis {analysis_id}")
    return analysis_id, project_igf_id
  except Exception as e:
    message = \
      f"Failed to get analysis_id and project_igf_id, error: {e}"
    log.error(message)
    raise ValueError(message)


def send_generic_logs_to_channels(
      ms_teams_conf: str,
      message_prefix: str,
      reaction: str,
      slack_conf: Optional[str] = None) -> None:
  """
  A function for sending generic logs to Slack and Teams along with the Airflow task log filepath

  Parameters:
  slack_conf (str): A file path containing Slack API tokens
  ms_teams_conf (str): A file path containing MS Teams webhook
  message_prefix (str): A custom text message
  reaction (str): A reaction string, either pass or faile

  Returns:
  None
  """
  try:
    context = get_current_context()
    log_file_path = [
      os.environ.get('AIRFLOW__LOGGING__BASE_LOG_FOLDER'),
      f"dag_id={context['ti'].dag_id}",
      f"run_id={context['ti'].run_id}",
      f"task_id={context['ti'].task_id}"]
    if str(context['ti'].map_index) != '-1':
      log_file_path.append(
        f"map_index={context['ti'].map_index}")
    log_file_path.append(
      f"attempt={context['ti'].try_number}.log")
    message = \
      f"{message_prefix}, Log: {os.path.join(*log_file_path)}"
    send_log_to_channels(
      slack_conf=slack_conf,
      ms_teams_conf=ms_teams_conf,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      project_id=None,
      comment=message,
      reaction=reaction)
  except Exception as e:
    log.error(e)


def send_airflow_failed_logs_to_channels(
    ms_teams_conf: str,
    message_prefix: str,
    slack_conf: Optional[str] = None) -> None:
  """
  A function for sending failed logs to Slack and Teams along with the Airflow task log filepath

  Parameters:
  slack_conf (str): A file path containing Slack API tokens
  ms_teams_conf (str): A file path containing MS Teams webhook
  message_prefix (str): A custom text message

  Returns:
  None
  """
  try:
    message_prefix = f"Error: {message_prefix}"
    send_generic_logs_to_channels(
      slack_conf=slack_conf,
      ms_teams_conf=ms_teams_conf,
      message_prefix=message_prefix,
      reaction='fail')
  except Exception as e:
    log.error(e)


def send_airflow_pipeline_logs_to_channels(
    ms_teams_conf: str,
    message_prefix: str,
    slack_conf: Optional[str] = None) -> None:
  """
  A function for sending logs to Slack and Teams along with the Airflow task log filepath

  Parameters:
  slack_conf (str): A file path containing Slack API tokens
  ms_teams_conf (str): A file path containing MS Teams webhook
  message_prefix (str): A custom text message

  Returns:
  None
  """
  try:
    message_prefix = f"MSG: {message_prefix}"
    send_generic_logs_to_channels(
      slack_conf=slack_conf,
      ms_teams_conf=ms_teams_conf,
      message_prefix=message_prefix,
      reaction='pass')
  except Exception as e:
    log.error(e)


def get_project_igf_id_for_analysis(
      analysis_id: int,
      dbconfig_file: str) \
        -> str:
  """
  Fetch project igf id from the input analysis id

  Parameters:
  analysis_id (int): Analysis id from analsis table
  dbconfig_file (str): Database config file path

  Returns:
  project_igf_id (str)
  """
  try:
    check_file_path(dbconfig_file)
    dbparams = read_dbconf_json(dbconfig_file)
    aa = AnalysisAdaptor(**dbparams)
    aa.start_session()
    project_igf_id = \
      aa.fetch_project_igf_id_for_analysis_id(
        analysis_id=analysis_id)
    aa.close_session()
    return project_igf_id
  except Exception as e:
    raise ValueError(
      f"Failed to get project_id for analysis {analysis_id}")


def fetch_analysis_name_for_analysis_id(
      analysis_id: int,
      dbconfig_file: str) -> str:
  """
  Fetch analysis name from analysis id

  Parameters:
  analysis_id (int): Analysis id from analsis table
  dbconfig_file (str): Database config file path

  Returns:
  analysis_name (str)
  """
  try:
    dbconf = read_dbconf_json(dbconfig_file)
    aa = AnalysisAdaptor(**dbconf)
    aa.start_session()
    analysis_entry = \
    aa.fetch_analysis_records_analysis_id(
      analysis_id=analysis_id,
      output_mode='one_or_none')
    aa.close_session()
    if analysis_entry is None:
      raise ValueError(
        f"No entry found for analysis id {analysis_id}")
    analysis_name = \
      analysis_entry.analysis_name
    if analysis_name is None:
      raise ValueError(
        f"Analysis name is None for id {analysis_id}")
    return analysis_name
  except Exception as e:
    raise ValueError(
      f"Failed to get analysis name for id {analysis_id}, error: {e}")


def fetch_analysis_type_for_analysis_id(
      analysis_id: int,
      dbconfig_file: str) -> str:
  """
  Fetch analysis type for analysis id

  Parameters:
  analysis_id (int): Analysis id from analsis table
  dbconfig_file (str): Database config file path

  Returns:
  analysis_type (str)
  """
  try:
    dbconf = read_dbconf_json(dbconfig_file)
    aa = AnalysisAdaptor(**dbconf)
    aa.start_session()
    analysis_entry = \
      aa.fetch_analysis_records_analysis_id(
        analysis_id=analysis_id,
        output_mode='one_or_none')
    aa.close_session()
    if analysis_entry is None:
      raise ValueError(
        f"No entry found for analysis id {analysis_id}")
    analysis_type = \
      analysis_entry.analysis_type
    if analysis_type is None:
      raise ValueError(
        f"Analysis type is None for id {analysis_id}")
    return analysis_type
  except Exception as e:
    raise ValueError(
      f"Failed to get analysis name for id {analysis_id}, error: {e}")


def fetch_user_info_for_project_igf_id(
      project_igf_id: str,
      dbconfig_file: str) -> Tuple[str, str, str, bool]:
  """
  Fetch primary user for any projects

  Parameters:
  project_igf_id (str): Project IGF id
  dbconfig_file (str): Database config file path

  Returns:
  user_name (str)
  login_name (str)
  user_email (str)
  hpcUser (str)
  """
  try:
    dbconf = read_dbconf_json(dbconfig_file)
    pa = ProjectAdaptor(**dbconf)
    pa.start_session()
    user_info = pa.get_project_user_info(project_igf_id=project_igf_id)
    pa.close_session()
    user_info = user_info[user_info['data_authority']=='T']
    user_info = user_info.to_dict(orient='records')
    if len(user_info) == 0:
      raise ValueError(
        f'No user found for project {project_igf_id}')
    user_info = user_info[0]
    user_name = user_info['name']
    login_name = user_info['username']
    user_email = user_info['email_id']
    user_category = user_info['category']
    hpcUser = False
    if user_category=='HPC_USER':
      hpcUser = True
    return user_name, login_name, user_email, hpcUser
  except Exception as e:
    raise ValueError(
      f"Failed to get user infor for projecty {project_igf_id}, error: {e}")


def generate_email_text_for_analysis(
      analysis_id: int,
      template_path: str,
      dbconfig_file: str,
      default_email_user: str,
      send_email_to_user: bool = True) -> Tuple[str, list]:
  """
  A function for generating email text for any analysis

  Parameters:
  analysis_id (int): Analysis id from analsis table
  template_path (str): Email template file path
  dbconfig_file (str): Database config file path
  default_email_user (str): Default user's email id
  send_email_to_user (bool): A toggle for sending email to primary user if "True" (default) or fall back to default user if "False"

  Returns:
  output_file (str)
  [user_email, default_email_user] (list)
  """
  try:
    ## get analysis name and project name
    project_igf_id = \
      get_project_igf_id_for_analysis(
        analysis_id=analysis_id,
        dbconfig_file=dbconfig_file)
    analysis_name = \
      fetch_analysis_name_for_analysis_id(
        analysis_id=analysis_id,
        dbconfig_file=dbconfig_file)
    ## get user info
    user_name, login_name, user_email, hpcUser = \
      fetch_user_info_for_project_igf_id(
        project_igf_id=project_igf_id,
        dbconfig_file=dbconfig_file)
    ## build email text file
    temp_dir = get_temp_dir(use_ephemeral_space=True)
    output_file = \
      os.path.join(temp_dir, 'email.txt')
    _create_output_from_jinja_template(
      template_file=template_path,
      output_file=output_file,
      autoescape_list=['xml', 'html'],
      data=dict(
      customerEmail=user_email,
      defaultUser=default_email_user,
      projectName=project_igf_id,
      analysisName=analysis_name,
      customerName=user_name,
      customerUsername=login_name,
      hpcUser=hpcUser,
      send_email_to_user=send_email_to_user))
    return output_file, [user_email, default_email_user]
  except Exception as e:
    raise ValueError(
      f"Failed to generate email body, error: {e}")


def format_and_send_email_to_user(
      email_template: str,
      email_config_file: str,
      analysis_id: int,
      database_config_file: str,
      email_user_key: str = 'username',
      send_email: bool = True) \
        -> None:
  """
  A function for formating and sending email about analysis pipeline status to user

  Parameters:
  email_template (str): Path of the email template file
  email_config_file (str): Path of the email config file
  analysis_id (int): Analysis id from the analysis table entry
  database_config_file (str): Database config file path
  email_user_key (str): Key for the default user as mentioned in the email config file, default is 'username'
  send_email (bool): A toggle for sending email to primary user if "True" (default) or fall back to default user if "False"

  Returns:
  None
  """
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
        dbconfig_file=database_config_file,
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
      send_email: bool = False,
      email_data: dict = {}) \
        -> None:
  """
  A function for sending generic email to users

  Parameters:
  user_name (str): Name of the user
  user_email (str): Email id of user
  email_template (str): Path of the email template file
  email_config_file (str): Path of the email config file
  email_user_key (str): Optional key for the default user as mentioned in the email config file, default is 'username'
  send_email (bool): Optional toggle for sending email to primary user if "True" (default) or fall back to default user if "False"
  email_data (dict): Optional data to be added to the email body as dictionary

  Returns:
  None
  """
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
    receivers = [default_email_user]
    if send_email:
      receivers.append(user_email)
    send_email_via_smtp(
      sender=default_email_user,
      receivers=receivers,
      email_config_json=email_config_file,
      email_text_file=output_file)
  except Exception as e:
    raise ValueError(
      f"Failed to send email, error: {e}")


def check_and_seed_analysis_pipeline(
      analysis_id: int,
      pipeline_name: str,
      dbconf_json_path: str,
      new_status: str,
      seed_table: str = 'analysis',
      create_new_pipeline_seed: bool = False,
      no_change_status: Optional[list] = None) \
        -> bool:
  """
  A function for checking and modifying pipeline seed table status

  Parameters:
  analysis_id (int): Analysis id from the analysis table entry
  pipeline_name (str): Name of the pipeline
  dbconf_json_path (str): Database config file path
  new_status (str): Set new status of the pipeline, should from this list: 'RUNNING', 'FINISHED', 'FAILED'
  seed_table (str): tanle name to seed pipeline, default 'analysis'
  create_new_pipeline_seed (bool): Optional flag to create new entry in the pipeline_seed table if its not present, default False
  no_change_status (list): Optional list to check current pipeline_seed table status and skip any change
  """
  try:
    dbconf = read_dbconf_json(dbconf_json_path)
    pa = PipelineAdaptor(**dbconf)
    try:
      pa.start_session()
      pipeline_exists = \
        pa.check_pipeline_using_pipeline_name(
          pipeline_name=pipeline_name)
      if not pipeline_exists:
        raise ValueError(
          f"Pipeline {pipeline_name} not registered in db")
      ## check if analysis exists
      aa = AnalysisAdaptor(**{'session': pa.session})
      analysis_id_exists = \
        aa.fetch_analysis_records_analysis_id(
          analysis_id=analysis_id,
          output_mode='one_or_none')
      if analysis_id_exists is None:
        raise ValueError(
          f'Analysis id {analysis_id} not found in db')
      ## check for existing analysis and pipeline seed combination
      if not create_new_pipeline_seed:
        existing_pipeline_seed = \
          pa.check_existing_pipeseed(
            seed_id=analysis_id,
            seed_table=seed_table,
            pipeline_name=pipeline_name)
        if existing_pipeline_seed is None:
          raise ValueError(
            f"No existing pipeline seed found for analysis {analysis_id} and pipeline {pipeline_name}")
      ## change seed status
      seed_status = \
        pa.create_or_update_pipeline_seed(
          seed_id=analysis_id,
          pipeline_name=pipeline_name,
          new_status=new_status,
          seed_table=seed_table,
          no_change_status=no_change_status,
          autosave=False)
      pa.commit_session()
      pa.close_session()
    except:
      pa.rollback_session()
      pa.close_session()
      raise
    return seed_status
  except Exception as e:
    raise ValueError(
      f"Failed to change analysis seed, error: {e}")


def fetch_analysis_design(
      analysis_id: int,
      pipeline_name: str,
      dbconfig_file: str) \
        -> str:
  """
  Fetch analysis design as json entry from DB and convert it to yaml string

  Parameters:
  analysis_id (int): Analysis id from the analysis table entry
  pipeline_name (str): Name of the pipeline
  dbconf_json_path (str): Database config file path

  Returns:
  input_design_yaml (str)
  """
  try:
    dbconf = read_dbconf_json(dbconfig_file)
    aa = AnalysisAdaptor(**dbconf)
    aa.start_session()
    input_design_yaml = ''
    try:
      analysis_entry = \
        aa.fetch_analysis_records_analysis_id(
        analysis_id=analysis_id,
        output_mode='one_or_none')
      if analysis_entry is None:
        raise ValueError(
          f"No entry found for analysis {analysis_id} in db")
      if analysis_entry.analysis_type is None or \
         analysis_entry.analysis_type != pipeline_name:
        raise ValueError(
          f"Analysis name mismatch: {pipeline_name} != {analysis_entry.analysis_type}")
      if analysis_entry.analysis_description is None:
        raise ValueError(
          f"Missing analysis_description for {analysis_id} and {pipeline_name}")
      input_design_yaml = \
        analysis_entry.analysis_description
      if isinstance(input_design_yaml, str):
        input_design_yaml = \
          yaml.dump(json.loads(input_design_yaml))
      if isinstance(input_design_yaml, dict):
        input_design_yaml = \
          yaml.dump(input_design_yaml)
      aa.close_session()
    except:
      aa.close_session()
      raise
    return input_design_yaml
  except Exception as e:
    raise ValueError(
      f"Failed to get analysis design for {analysis_id} and {pipeline_name}")


def fetch_analysis_yaml_and_dump_to_a_file(
      analysis_id: int,
      pipeline_name: str,
      dbconfig_file: str) -> str:
  """
  Fetch analysis design from database and dump it to a yaml file

  Parameters:
  analysis_id (int): Analysis id from the analysis table entry
  pipeline_name (str): Name of the pipeline
  dbconf_json_path (str): Database config file path

  Returns:
  temp_yaml_file (str)

  """
  try:
    ## get analysis design
    input_design_yaml = \
      fetch_analysis_design(
        analysis_id=analysis_id,
        pipeline_name=pipeline_name,
        dbconfig_file=dbconfig_file)
    temp_dir = \
      get_temp_dir(use_ephemeral_space=True)
    temp_yaml_file = \
      os.path.join(temp_dir, 'analysis_design.yaml')
    ## dump it in a text file for next task
    with open(temp_yaml_file, 'w') as fp:
      fp.write(input_design_yaml)
    return temp_yaml_file
  except Exception as e:
    message = f"Failed to get yaml, error: {e}"
    raise ValueError(message)


def load_analysis_and_build_collection(
      collection_name: str,
      collection_type: str,
      collection_table: str,
      dbconfig_file: str,
      analysis_id: int,
      result_dir: str,
      hpc_base_path: str,
      date_tag: str,
      pipeline_name: Optional[str] = None,
      ignore_dangling_symlinks: bool = True,
      analysis_dir_prefix: str = 'analysis') \
        -> str:
  """
  A function for copying all the files present under a result directory to 
  a directory structure and creating an entry in the collection-file table

  Parameters:
  collection_name (str): Name to use for file collection
  collection_type (str): Type of the files
  collection_table (str): DB table name to link to the file collection
  dbconfig_file (str): Database config file path
  analysis_id (int): Analysis id from the analysis table entry
  pipeline_name (Optional[str]): Pipeline name
  result_dir (str): Input directory path
  hpc_base_path (str): target base directory
  date_tag (str): Date tag to mark unique dataset path
  ignore_dangling_symlinks (bool): Ignore danglig symlink setting for `shutils.copytree`,
                                   default is True
  analysis_dir_prefix (str):  Specific tag for analysis directoy structure,
                              default is 'analysis'

  Returns:
  target_dir_path (str)
  """
  try:
    check_file_path(result_dir)
    check_file_path(hpc_base_path)
    check_file_path(dbconfig_file)
    if pipeline_name is None:
      pipeline_name = \
        fetch_analysis_type_for_analysis_id(
          analysis_id=analysis_id,
          dbconfig_file=dbconfig_file)
    project_igf_id = \
      get_project_igf_id_for_analysis(
        analysis_id=analysis_id,
        dbconfig_file=dbconfig_file)
    analysis_name = \
      fetch_analysis_name_for_analysis_id(
        analysis_id=analysis_id,
        dbconfig_file=dbconfig_file)
    ## get project id
    # dbconf = read_dbconf_json(dbconfig_file)
    # aa = AnalysisAdaptor(**dbconf)
    # aa.start_session()
    # project_igf_id = \
    #   aa.fetch_project_igf_id_for_analysis_id(
    #     analysis_id=analysis_id)
    # analysis_name = None
    # analysis_entry = \
    #   aa.fetch_analysis_records_analysis_id(
    #     analysis_id=analysis_id,
    #     output_mode='one_or_none')
    # if analysis_entry is None:
    #   raise ValueError(
    #     f"No entry found for analysis {analysis_id} in db")
    # analysis_name = \
    #   analysis_entry.analysis_name
    # if analysis_name is None:
    #   raise ValueError(
    #     f"No analysis_name found for analysis {analysis_id} in db")
    # aa.close_session()
    ## move analysis to hpc. This can take long time.
    target_dir_path = \
      os.path.join(
        hpc_base_path,
        project_igf_id,
        analysis_dir_prefix,
        pipeline_name,
        analysis_name,
        date_tag,
        os.path.basename(result_dir))
    if os.path.exists(target_dir_path):
      raise ValueError(
        f"Output path {target_dir_path} already present. Manually remove it before re-run.")
    copy_local_file(
      source_path=result_dir,
      destination_path=target_dir_path,
      ignore_dangling_symlinks=ignore_dangling_symlinks)
    check_file_path(target_dir_path)
    ## load analysis to db
    collection_data_list = [{
      'name': collection_name,
      'type': collection_type,
      'table': collection_table,
      'file_path': target_dir_path}]
    dbconf = read_dbconf_json(dbconfig_file)
    ca = CollectionAdaptor(**dbconf)
    ca.start_session()
    try:
      ca.load_file_and_create_collection(
        data=collection_data_list,
        calculate_file_size_and_md5=False,
        autosave=False)
      ca.commit_session()
      ca.close_session()
    except:
      ca.rollback_session()
      ca.close_session()
      raise
    return target_dir_path
  except Exception as e:
    raise ValueError(
      f"Failed to load analysis results from {result_dir}, error: {e}")


def parse_analysis_design_and_get_metadata(
      input_design_yaml: str,
      sample_metadata_key: str = 'sample_metadata',
      analysis_metadata_key: str = 'analysis_metadata') \
      -> Tuple[Optional[dict], Optional[dict]]:
  """
  A function for parsing analysis design yaml string data
  and fetching `sample_metadata` and `analysis_metadata` entries

  Parameters:
  input_design_yaml (str): YAML analysis design as string
  sample_metadata_key (str): Name of the sample metadata key, default `sample_metadata`
  analysis_metadata_key (str): Name of the analysis metadata key, default `analysis_metadata`

  Returns:
  sample_metadata (dict)
  analysis_metadata (dict)
  """
  try:
    yaml_data = yaml.safe_load(input_design_yaml)
    sample_metadata = \
      yaml_data.get(sample_metadata_key)
    analysis_metadata = \
      yaml_data.get(analysis_metadata_key)
    return sample_metadata, analysis_metadata
  except Exception as e:
    raise ValueError(
      f"Failed to parse analysis design, error: {e}")


def copy_analysis_to_globus_dir(
      globus_root_dir: str,
      dbconfig_file: str,
      analysis_id: int,
      analysis_dir: str,
      date_tag: str,
      ignore_dangling_symlinks: bool = True,
      analysis_dir_prefix: str = 'analysis') \
        -> str:
  """
  A function for copying files to Globus analysis directory

  Parameters:
  globus_root_dir (str): Globus root directory path
  dbconfig_file (str): Database config file path
  analysis_id (int): Analysis id from the analysis table entry
  analysis_dir (str): Analysis directory path for copy
  date_tag (str): Date tag for unique target direcoty path
  ignore_dangling_symlinks (bool): Ignore danglig symlink setting for `shutils.copytree`,
                                   default is True
  analysis_dir_prefix (str): Prefix for analysis directory, default is `analysis`

  Returns:
  target_dir_path (str)
  """
  try:
    check_file_path(globus_root_dir)
    ## get pipeline name
    pipeline_name = \
      fetch_analysis_type_for_analysis_id(
        analysis_id=analysis_id,
        dbconfig_file=dbconfig_file)
    ## fetch analysis name
    analysis_name = \
      fetch_analysis_name_for_analysis_id(
        analysis_id=analysis_id,
        dbconfig_file=dbconfig_file)
    ## get project name
    project_igf_id = \
      get_project_igf_id_for_analysis(
        analysis_id=analysis_id,
        dbconfig_file=dbconfig_file)
    ## build target path
    target_dir_path = \
      os.path.join(
        globus_root_dir,
        project_igf_id,
        analysis_dir_prefix,
        pipeline_name,
        analysis_name,
        date_tag,
        os.path.basename(analysis_dir))
    if os.path.exists(target_dir_path):
      raise ValueError(
        f"Globus target dir {target_dir_path} already present")
    ## copy directory
    copy_local_file(
      source_path=analysis_dir,
      destination_path=target_dir_path,
      ignore_dangling_symlinks=ignore_dangling_symlinks)
    check_file_path(target_dir_path)
    return target_dir_path
  except Exception as e:
    raise ValueError(
      f"Failed to copy data to globus dir, error: {e}")


def get_fastq_for_samples_and_dump_in_json_file(
      design_file: str,
      db_config_file: str) -> str:
  """
  A function for fetching sample and fastq records for analysis design
  Sample record includes sample_igf_id, run_igf_id, flowcell_id, lane_number and file_paths

  Parameters:
  design_file (str): Path of a analysis design yaml file
  db_config_file (str): Database config path

  Returns:
  fastq_list_json (str)
  """
  try:
    check_file_path(design_file)
    with open(design_file, 'r') as fp:
      input_design_yaml = fp.read()
    sample_metadata, analysis_metadata = \
      parse_analysis_design_and_get_metadata(
        input_design_yaml=input_design_yaml)
    if sample_metadata is None or \
       analysis_metadata is None:
      raise KeyError(
        "Missing sample or analysis metadata")
    ## get sample ids from metadata
    sample_igf_id_list = \
      list(sample_metadata.keys())
    if len(sample_igf_id_list) == 0:
      raise ValueError(
        "No sample id found in the metadata")
    ## get fastq files for all samples
    fastq_list = \
      get_fastq_and_run_for_samples(
        dbconfig_file=db_config_file,
        sample_igf_id_list=sample_igf_id_list)
    if len(fastq_list) == 0:
      raise ValueError(
        f"No fastq file found for samples: {design_file}")
    temp_dir = \
      get_temp_dir(use_ephemeral_space=True)
    fastq_list_json = \
      os.path.join(temp_dir, 'fastq_list.json')
    with open(fastq_list_json, 'w') as fp:
      json.dump(fastq_list, fp)
    return fastq_list_json
  except Exception as e:
    raise ValueError(
      f"Failed to create fastq list json, error: {e}")


def calculate_analysis_name(
      analysis_id: int,
      date_tag: str,
      dbconfig_file: str) \
        -> str:
  """
  Calculate analysis name

  Parameters:
  analysis_id (int): Analysis id from analysis table
  date_tag (str): Date tag
  dbconfig_file (str): Database config file

  Returns:
  collection_name (str)
  """
  try:
    check_file_path(dbconfig_file)
    ## fetch analysis name
    analysis_name = \
      fetch_analysis_name_for_analysis_id(
        analysis_id=analysis_id,
        dbconfig_file=dbconfig_file)
    symbol_pattern = \
      re.compile(r"[!\"#$%&\[\]\\'()*\+,./:;<=>?@^`{|}~]")
    white_space_pattern = \
      re.compile(r'\s+')
    double_underscore = \
      re.compile(r'_+')
    s1 = re.sub(symbol_pattern, '_', analysis_name)
    s2 = re.sub(white_space_pattern, '_', s1)
    analysis_name = re.sub(double_underscore, '_', s2)
    collection_name = \
      f"{analysis_name}_{str(analysis_id)}_{date_tag}"
    collection_name = \
      re.sub(double_underscore, '_', collection_name)
    return collection_name
  except Exception as e:
    raise ValueError(
      f"Failed to calculate analysis name for entry {analysis_id}, error: {e}")


def collect_analysis_dir(
      analysis_id: int,
      dag_name: str,
      dir_path: str,
      db_config_file:str,
      hpc_base_path: str,
      collection_table: str = 'analysis',
      analysis_dir_prefix: str = 'analysis') -> Tuple[str, str, str]:
  try:
    date_tag = get_date_stamp_for_file_name()
    collection_type = dag_name.upper()
    collection_name = \
      calculate_analysis_name(
        analysis_id=analysis_id,
        date_tag=date_tag,
        dbconfig_file=db_config_file)
    target_dir_path = \
      load_analysis_and_build_collection(
        collection_name=collection_name,
        collection_type=collection_type,
        collection_table=collection_table,
        dbconfig_file=db_config_file,
        analysis_id=analysis_id,
        pipeline_name=dag_name,
        result_dir=dir_path,
        hpc_base_path=hpc_base_path,
        analysis_dir_prefix=analysis_dir_prefix,
        date_tag=date_tag)
    ## get project name
    project_igf_id = \
      get_project_igf_id_for_analysis(
        analysis_id=analysis_id,
        dbconfig_file=db_config_file)
    return target_dir_path, project_igf_id, date_tag
  except Exception as e:
    raise ValueError(
      f"Failed to collect analysis dir, error: {e}")


def calculate_md5sum_for_analysis_dir(dir_path: str) -> str:
  try:
    temp_dir = \
      get_temp_dir(use_ephemeral_space=True)
    bash_template = \
      Template(
        """set -eo pipefail;
        cd {{ TMP_PATH }};
        find {{ DIR_PATH }} -type f -exec md5sum {} \; > file_manifest.md5;
        mv file_manifest.md5 {{ DIR_PATH }}""")
    script_path = \
      os.path.join(temp_dir, 'bash_script.sh')
    rendered_template = \
      bash_template.render(
        TMP_PATH=temp_dir,
        DIR_PATH=dir_path)
    with open(script_path, 'w') as fp:
      fp.write(rendered_template)
    stdout_file, stderr_file = \
      bash_script_wrapper(
        script_path=script_path)
    md5_sum_file = \
      os.path.join(dir_path, 'file_manifest.md5')
    check_file_path(md5_sum_file)
    return md5_sum_file
  except Exception as e:
    raise ValueError(
      f"Failed to get md5sum for dir {dir_path}, error: {e}")


def get_per_sample_analysis_groups(design_file: str) -> list:
  try:
    check_file_path(design_file)
    with open(design_file, 'r') as fp:
      input_design_yaml = fp.read()
      sample_metadata, analysis_metadata = \
        parse_analysis_design_and_get_metadata(
          input_design_yaml=input_design_yaml)
    if sample_metadata is None or \
       analysis_metadata is None:
      raise KeyError(
        "Missing sample or analysis metadata")
    unique_sample_groups = list()
    for sample_name, sample_data in sample_metadata.items():
      unique_sample_groups.\
        append({
          "sample_metadata": {
            sample_name: sample_data},
          "analysis_metadata": analysis_metadata})
    if len(unique_sample_groups) == 0:
      raise ValueError("No sample group found")
    return unique_sample_groups
  except Exception as e:
    raise ValueError(
      f"Failed to get groups for analysis, error: {e}")