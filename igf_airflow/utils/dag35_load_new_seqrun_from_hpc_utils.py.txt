import os
import stat
import json
import base64
import logging
import pandas as pd
from typing import Tuple, Any, Optional
from airflow.models import Variable
from airflow.models import taskinstance
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_airflow.logging.upload_log_msg import send_log_to_channels
from igf_data.utils.fileutils import check_file_path
from igf_data.illumina.runinfo_xml import RunInfo_xml
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.utils.fileutils import (
  get_temp_dir,
  get_date_stamp,
  copy_local_file,
  get_date_stamp_for_file_name)
from igf_portal.api_utils import upload_files_to_portal
from igf_data.illumina.runparameters_xml import RunParameter_xml
from igf_airflow.utils.dag22_bclconvert_demult_utils import _create_output_from_jinja_template
from igf_data.utils.jupyter_nbconvert_wrapper import Notebook_runner
from igf_portal.api_utils import upload_files_to_portal
from igf_airflow.utils.dag25_copy_seqruns_to_hpc_utils import (
    register_new_seqrun_to_db,
    _create_interop_report,
    _load_interop_data_to_db,
    _load_interop_overview_data_to_seqrun_attribute)


log = logging.getLogger(__name__)

SLACK_CONF = Variable.get('slack_conf',default_var=None)
MS_TEAMS_CONF = Variable.get('ms_teams_conf',default_var=None)
DATABASE_CONFIG_FILE = Variable.get('database_config_file', default_var=None)
HPC_SEQRUN_PATH = Variable.get('hpc_seqrun_path', default_var=None)
IGF_PORTAL_CONF = Variable.get('igf_portal_conf', default_var=None)
PORTAL_ADD_SEQRUN_URL = "/api/v1/raw_seqrun/add_new_seqrun"
PORTAL_ADD_INTEROP_REPORT_URL = "/api/v1/interop_data/add_report"
HPC_INTEROP_PATH = Variable.get('hpc_interop_path', default_var=None)
INTEROP_REPORT_TEMPLATE = Variable.get('interop_report_template', default_var=None)
INTEROP_REPORT_IMAGE = Variable.get('interop_report_image', default_var=None)
INTEROP_REPORT_BASE_PATH = Variable.get('interop_report_base_path', default_var=None)


def register_run_to_db_and_portal_func(**context):
  try:
    ti = context.get('ti')
    server_in_use = \
      context['params'].get('server_in_use')
    xcom_key = \
      context['params'].get('xcom_key', 'seqrun_id')
    wells_xcom_task = \
      context['params'].get('wells_xcom_task', 'get_new_seqrun_id_from_wells')
    orwell_xcom_task = \
      context['params'].get('orwell_xcom_task', 'get_new_seqrun_id_from_orwell')
    seqrun_id = None
    if server_in_use.lower() == 'orwell':
      seqrun_id = \
        ti.xcom_pull(
          task_ids=orwell_xcom_task,
          key=xcom_key)
    elif server_in_use.lower() == 'wells':
      seqrun_id = \
        ti.xcom_pull(
          task_ids=wells_xcom_task,
          key=xcom_key)
    else:
      raise ValueError(f"Invalide server name {server_in_use}")
    if seqrun_id is None:
      raise ValueError('Missing seqrun id')
    ## register seqrun id to production db
    _ = \
      register_new_seqrun_to_db(
        dbconfig_file=DATABASE_CONFIG_FILE,
        seqrun_id=seqrun_id,
        seqrun_base_path=HPC_SEQRUN_PATH)
    ## register seqrun id to portal db
    temp_dir = \
      get_temp_dir(use_ephemeral_space=True)
    ## get read stats
    runinfo_file_path = \
      os.path.join(
        HPC_SEQRUN_PATH,
        seqrun_id,
        'RunInfo.xml')
    check_file_path(runinfo_file_path)
    runinfo_data = \
      RunInfo_xml(
        xml_file=runinfo_file_path)
    formatted_read_stats = \
      runinfo_data.\
        get_formatted_read_stats()
    json_data = {
      "seqrun_id_list": [seqrun_id,],
      "run_config_list": [formatted_read_stats,]}
    new_run_list_json = \
      os.path.join(
        temp_dir,
        'new_run_list.json')
    with open(new_run_list_json, 'w') as fp:
      json.dump(json_data, fp)
    check_file_path(new_run_list_json)
    res = \
      upload_files_to_portal(
        url_suffix=PORTAL_ADD_SEQRUN_URL,
        portal_config_file=IGF_PORTAL_CONF,
        file_path=new_run_list_json,
        verify=False,
        jsonify=False)
  except Exception as e:
    log.error(e)
    log_file_path = [
      os.environ.get('AIRFLOW__LOGGING__BASE_LOG_FOLDER'),
      f"dag_id={ti.dag_id}",
      f"run_id={ti.run_id}",
      f"task_id={ti.task_id}",
      f"attempt={ti.try_number}.log"]
    message = \
      f"Error: {e}, Log: {os.path.join(*log_file_path)}"
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise

def generate_interop_report_and_upload_to_portal_func(**context):
  try:
    ti = context.get('ti')
    server_in_use = \
      context['params'].get('server_in_use')
    xcom_key = \
      context['params'].get('xcom_key', 'seqrun_id')
    wells_xcom_task = \
      context['params'].get('wells_xcom_task', 'get_new_seqrun_id_from_wells')
    orwell_xcom_task = \
      context['params'].get('orwell_xcom_task', 'get_new_seqrun_id_from_orwell')
    seqrun_id = None
    if server_in_use.lower() == 'orwell':
      seqrun_id = \
        ti.xcom_pull(
          task_ids=orwell_xcom_task,
          key=xcom_key)
    elif server_in_use.lower() == 'wells':
      seqrun_id = \
        ti.xcom_pull(
          task_ids=wells_xcom_task,
          key=xcom_key)
    else:
      raise ValueError(f"Invalide server name {server_in_use}")
    if seqrun_id is None:
      raise ValueError('Missing seqrun id')
    output_notebook_path, metrics_dir, overview_csv_output, tile_parquet_output, work_dir = \
      _create_interop_report(
        run_id=seqrun_id,
        run_dir_base_path=HPC_SEQRUN_PATH,
        report_template=INTEROP_REPORT_TEMPLATE,
        report_image=INTEROP_REPORT_IMAGE,
        extra_container_dir_list=['/apps',])
    _load_interop_data_to_db(
      run_id=seqrun_id,
      interop_output_dir=work_dir,
      interop_report_base_path=INTEROP_REPORT_BASE_PATH,
      dbconfig_file=DATABASE_CONFIG_FILE)
    _load_interop_overview_data_to_seqrun_attribute(
      seqrun_igf_id=seqrun_id,
      dbconfig_file=DATABASE_CONFIG_FILE,
      interop_overview_file=overview_csv_output)
    res = \
      upload_files_to_portal(
        portal_config_file=IGF_PORTAL_CONF,
        file_path=output_notebook_path,
        data={"run_name": seqrun_id, "tag": "InterOp"},
        url_suffix=PORTAL_ADD_INTEROP_REPORT_URL)
  except Exception as e:
    log.error(e)
    log_file_path = [
      os.environ.get('AIRFLOW__LOGGING__BASE_LOG_FOLDER'),
      f"dag_id={ti.dag_id}",
      f"run_id={ti.run_id}",
      f"task_id={ti.task_id}",
      f"attempt={ti.try_number}.log"]
    message = \
      f"Error: {e}, Log: {os.path.join(*log_file_path)}"
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise