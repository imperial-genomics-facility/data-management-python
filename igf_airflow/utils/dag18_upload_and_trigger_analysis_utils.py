from airflow.models import Variable
import logging, os, requests, subprocess, re, shutil, gzip
from igf_airflow.logging.upload_log_msg import send_log_to_channels
from igf_data.utils.fileutils import check_file_path, get_temp_dir, copy_local_file, get_datestamp_label
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.fileadaptor import FileAdaptor

DATABASE_CONFIG_FILE = \
  Variable.get('database_config_file', default_var=None)
SLACK_CONF = \
  Variable.get('slack_conf', default_var=None)
MS_TEAMS_CONF = \
  Variable.get('ms_teams_conf', default_var=None)
ANALYSIS_LOOKUP_DIR = \
  Variable.get("analysis_lookup_dir", default_var=None)
ANALYSIS_TRIGGER_FILE = \
  Variable.get("analysis_triger_file", default_var=None)

def load_analysis_design_func(**context):
  try:
    task_index = \
      context['params'].get('task_index')
    load_design_xcom_key = \
      context['params'].get('load_design_xcom_key')
    load_design_xcom_task = \
      context['params'].get('load_design_xcom_task')
    ti = context.get('ti')
    analysis_files = \
      ti.xcom_pull(
        task_ids=load_design_xcom_task,
        key=load_design_xcom_key)
    analysis_file = \
      analysis_files.get(task_index)
    if analysis_file is None:
      raise ValueError("No analysis file list found")
    check_file_path(analysis_file)

  except Exception as e:
    logging.error(e)
    message = \
      'analysis input loading error: {0}'.\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise

def find_all_analysis_yaml_files(analysis_design_path):
  try:
    check_file_path(analysis_design_path)
    all_yaml_files = list()
    for root, _, files in os.walk(analysis_design_path):
      for f in files:
        if f.endswith(".yaml") or \
           f.endswith(".yml"):
          file_path = \
            os.path.join(root, f)
          all_yaml_files.\
            append(file_path)
    return all_yaml_files
  except Exception as e:
    raise ValueError(
            "Failed to list analysis files in {0}, error: {1}".\
              format(analysis_design_path, e))

def get_new_file_list(all_files, db_config_file):
  try:
    filtered_list = list()
    if isinstance(all_files, list) and \
       len(all_files) > 0:
      db_params = \
        read_dbconf_json(db_config_file)
      fa = FileAdaptor(**db_params)
      fa.start_session()
      for f in all_files:
        file_exists = \
          fa.check_file_records_file_path(f)
        if not file_exists:
          filtered_list.append(f)
    return filtered_list
  except Exception as e:
    raise ValueError(
            "Failed to check db for existing file, error: {0}".format(e))

def find_analysis_designs_func(**context):
  try:
    load_analysis_task_prefix = \
      context['params'].get('load_analysis_task_prefix')
    load_task_limit = \
      context['params'].get('load_task_limit')
    load_design_xcom_key = \
      context['params'].get('load_design_xcom_key')
    no_task_name = \
      context['params'].get('no_task_name')
    ti = context.get('ti')
    all_files = \
      find_all_analysis_yaml_files(
          ANALYSIS_LOOKUP_DIR)
    new_files = \
      get_new_file_list(
          all_files,
          DATABASE_CONFIG_FILE)
    if len(new_files) > load_task_limit:
      new_files = new_files[0: load_task_limit]                                 # loading only 20 files, its ok as we never going to get that many
    if len(new_files) > 0:
      task_list = [
        "{0}_{1}".format(load_analysis_task_prefix, i)
          for i in range(0, len(new_files))]
      ti.xcom_push(
        key=load_design_xcom_key,
        value=new_files)
    else:
      task_list = [no_task_name]
    return task_list
  except Exception as e:
    logging.error(e)
    message = \
      'analysis input finding error: {0}'.\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise