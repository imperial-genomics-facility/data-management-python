import os, re, logging
import pandas as pd
from airflow.models import Variable
from igf_data.utils.fileutils import get_temp_dir
from igf_data.utils.fileutils import remove_dir
from igf_data.utils.fileutils import copy_local_file
from igf_data.utils.fileutils import copy_remote_file
from igf_airflow.logging.upload_log_msg import send_log_to_channels
from igf_portal.metadata_utils import get_db_data_and_create_json_dump
from igf_portal.metadata_utils import get_raw_metadata_from_lims_and_load_to_portal
from igf_portal.api_utils import upload_files_to_portal
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.igfTables import Project
from igf_data.utils.fileutils import check_file_path
from igf_data.utils.singularity_run_wrapper import singularity_run
from igf_data.process.metadata_reformat.reformat_metadata_file import Reformat_metadata_file


DATABASE_CONFIG_FILE = \
  Variable.get('database_config_file', default_var=None)
SLACK_CONF = \
  Variable.get('slack_conf', default_var=None)
MS_TEAMS_CONF = \
  Variable.get('ms_teams_conf', default_var=None)
IGF_PORTAL_CONF = \
  Variable.get('igf_portal_conf', default_var=None)



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
      'Failed metadata dump, error: {0}'.\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise


def upload_metadata_to_portal_db_func(**context):
  try:
    ti = context.get('ti')
    json_dump_xcom_key = \
      context['params'].get('json_dump_xcom_key')
    json_dump_xcom_task = \
      context['params'].get('json_dump_xcom_task')
    data_load_url = \
      context['params'].get('data_load_url', '/api/v1/metadata/load_metadata')
    json_dump_file = \
      ti.xcom_pull(
        task_ids=json_dump_xcom_task,
        key=json_dump_xcom_key)
    check_file_path(json_dump_file)
    upload_files_to_portal(
      portal_config_file=IGF_PORTAL_CONF,
      file_path=json_dump_file,
      url_suffix=data_load_url)
    os.remove(json_dump_file)
  except Exception as e:
    logging.error(e)
    message = \
      'Failed to upload metadata to portal db, error: {0}'.\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise


def copy_remote_file_to_hpc_func(**context):
  try:
    ti = context.get('ti')
    xcom_key = \
      context['params'].get('xcom_key')
    source_user = \
      context['params'].get('source_user')
    source_address = \
      context['params'].get('source_address')
    source_path = \
      context['params'].get('source_path')
    temp_dir = \
      get_temp_dir(
        use_ephemeral_space=True)
    dest_path = \
      os.path.join(
        temp_dir,
        os.path.basename(source_path))
    copy_remote_file(
      source_path=source_path,
      destination_path=dest_path,
      source_address='{0}@{1}'.format(source_user, source_address),
      destination_address=None,
      copy_method='rsync',
      check_file=True,
      force_update=True)
    ti.xcom_push(
      key=xcom_key,
      value=dest_path)
  except Exception as e:
    logging.error(e)
    message = \
      "Failed to copy remote file, error: {0}".\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise

def _get_all_known_projects(db_conf_file):
  try:
    check_file_path(db_conf_file)
    temp_dir = \
      get_temp_dir(
        use_ephemeral_space=True)
    temp_file = \
      os.path.join(
        temp_dir, 'project_list.txt')
    dbparam = read_dbconf_json(db_conf_file)
    pa = ProjectAdaptor(**dbparam)
    pa.start_session()
    project_list = pa.fetch_all_project_igf_ids()
    pa.close_session()
    if not isinstance(project_list, pd.DataFrame):
      raise TypeError(
              "Expecting a Pandas DataFrame, got: {0}".\
                format(type(project_list)))
    project_list.\
      to_csv(temp_file, index=False)
    check_file_path(temp_file)
    return temp_file
  except Exception as e:
    raise ValueError("Failed to get project list, error: {0}".format(e))


def get_known_projects_func(**context):
  try:
    ti = context.get('ti')
    xcom_key = \
      context['params'].get('xcom_key')
    project_list_file = \
      _get_all_known_projects(
        db_conf_file=DATABASE_CONFIG_FILE)
    ti.xcom_push(
      key=xcom_key,
      value=project_list_file)
  except Exception as e:
    logging.error(e)
    message = \
      "Failed to get known project list, error: {0}".\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise


def create_raw_metadata_for_new_projects_func(**context):
  try:
    ti = context.get('ti')
    xcom_key = \
      context['params'].get('xcom_key')
    quota_xcom_task = \
      context['params'].get('quota_xcom_task')
    quota_xcom_key = \
      context['params'].get('quota_xcom_key')
    access_db_xcom_task = \
      context['params'].get('access_db_xcom_task')
    access_db_xcom_key = \
      context['params'].get('access_db_xcom_key')
    known_projects_xcom_task = \
      context['params'].get('known_projects_xcom_task')
    known_projects_xcom_key = \
      context['params'].get('known_projects_xcom_key')
    spark_threads = \
      context['params'].get('spark_threads')
    spark_py_file = \
      context['params'].get('spark_py_file')
    spark_script_path = \
      context['params'].get('spark_script_path')
    ucanaccess_path = \
      context['params'].get('ucanaccess_path')
    singularity_image_path = \
      Variable.get("spark_lims_metadata_image")
    quota_xlsx = \
      ti.xcom_pull(
        task_ids=quota_xcom_task,
        key=quota_xcom_key)
    check_file_path(quota_xlsx)
    access_db = \
      ti.xcom_pull(
        task_ids=access_db_xcom_task,
        key=access_db_xcom_key)
    check_file_path(access_db)
    project_list = \
      ti.xcom_pull(
        task_ids=known_projects_xcom_task,
        key=known_projects_xcom_key)
    check_file_path(project_list)
    temp_dir = \
      get_temp_dir(use_ephemeral_space=True)
    path_bind = [
      temp_dir,
      os.path.dirname(quota_xlsx),
      os.path.dirname(access_db),
      os.path.dirname(project_list)]
    run_args = [
      'spark-submit',
      '--master local[{0}]'.format(int(spark_threads)),
      '--py-files {0}'.format(spark_py_file),
      '{0} -a {1} -q {2} -o {3} -k {4} -j {5}'.\
        format(
          spark_script_path,
          access_db,
          quota_xlsx,
          temp_dir,
          project_list,
          ucanaccess_path)]
    options = [
      "--no-home",
      "-C"]
    _, _ = \
      singularity_run(
        image_path=singularity_image_path,
        bind_dir_list=path_bind,
        args_list=run_args,
        options=options)
    ti.xcom_push(
      key=xcom_key,
      value=temp_dir)
  except Exception as e:
    logging.error(e)
    message = \
      "Failed to get new metadata files, error: {0}".\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise


def _reformat_metadata_files(input_dir):
  try:
    temp_dir = \
      get_temp_dir(
        use_ephemeral_space=True)
    check_file_path(input_dir)
    samplesheet_pattern = \
      re.compile(r'\S+_SampleSheet\S+')
    reformatted_pattern = \
      re.compile(r'\S+_reformatted\S+')
    for entry in os.listdir(path=input_dir):
      entry = os.path.join(input_dir, entry)
      if os.path.isfile(entry) and \
         entry.endswith('.csv') and \
         not re.match(reformatted_pattern,os.path.basename(entry)):
         if not re.match(samplesheet_pattern, entry):
          output_file = \
            os.path.basename(entry).\
              replace('.csv', '_reformatted.csv')
          formatted_dest_path = \
            os.path.join(
              input_dir,
              'formatted_data')
          output_file = \
            os.path.join(
              temp_dir,
              output_file)
          re_metadata = \
            Reformat_metadata_file(\
              infile=entry)
          re_metadata.\
            reformat_raw_metadata_file(
              output_file=output_file)
          copy_local_file(
            output_file,
            os.path.join(
              formatted_dest_path,
              os.path.basename(output_file)),
            force=True)
    remove_dir(temp_dir)
  except Exception as e:
    raise ValueError(
            "Failed to reformat metadata files, error: {0}".\
              format(e))


def get_formatted_metadata_files_func(**context):
  try:
    ti = context.get('ti')
    xcom_key = \
      context['params'].get('xcom_key')
    raw_metadata_xcom_key = \
      context['params'].get('raw_metadata_xcom_key')
    raw_metadata_xcom_task = \
      context['params'].get('raw_metadata_xcom_task')
    raw_metadata_dir = \
      ti.xcom_pull(
        task_ids=raw_metadata_xcom_task,
        key=raw_metadata_xcom_key)
    _reformat_metadata_files(
      input_dir=raw_metadata_dir)
    ti.xcom_push(
      key=xcom_key,
      value=raw_metadata_dir)
  except Exception as e:
    logging.error(e)
    message = \
      "Failed to get formatted metadata files, error: {0}".\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise


def upload_raw_metadata_to_portal_func(**context):
  try:
    ti = context.get('ti')
    formatted_metadata_xcom_key = \
      context['params'].get('formatted_metadata_xcom_key')
    formatted_metadata_xcom_task = \
      context['params'].get('formatted_metadata_xcom_task')
    formatted_metadata = \
      ti.xcom_pull(
        task_ids=formatted_metadata_xcom_task,
        key=formatted_metadata_xcom_key)
    check_file_path(formatted_metadata)
    get_raw_metadata_from_lims_and_load_to_portal(
      metadata_dir=formatted_metadata,
      portal_conf_file=IGF_PORTAL_CONF)
  except Exception as e:
    logging.error(e)
    message = \
      "Failed to upload raw metadata to portal, error: {0}".\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise

