import os
import stat
import json
import base64
import logging
import pandas as pd
from typing import Tuple, Any
from airflow.models import Variable
from airflow.models import taskinstance
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_airflow.logging.upload_log_msg import send_log_to_channels
from igf_data.utils.fileutils import check_file_path
from igf_data.illumina.runinfo_xml import RunInfo_xml
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.utils.fileutils import get_temp_dir
from igf_portal.api_utils import upload_files_to_portal
from igf_data.illumina.runparameters_xml import RunParameter_xml

log = logging.getLogger(__name__)

SLACK_CONF = Variable.get('slack_conf',default_var=None)
MS_TEAMS_CONF = Variable.get('ms_teams_conf',default_var=None)
DATABASE_CONFIG_FILE = Variable.get('database_config_file', default_var=None)
HPC_SEQRUN_PATH = Variable.get('hpc_seqrun_path', default_var=None)
IGF_PORTAL_CONF = Variable.get('igf_portal_conf', default_var=None)
PORTAL_ADD_SEQRUN_URL = "/api/v1/raw_seqrun/add_new_seqrun"

def get_new_run_id_for_copy(**context):
  try:
    ti = context.get('ti')
    xcom_task = \
      context['params'].get('xcom_task')
    next_task = \
      context['params'].get('next_task')
    no_work_task = \
      context['params'].get('no_work_task')
    seqrun_id_xcom_key = \
      context['params'].get('seqrun_id_xcom_key')
    run_lists = \
      ti.xcom_pull(task_ids=xcom_task)
    if isinstance(run_lists, bytes):
      run_lists = run_lists.decode('utf-8')
    if isinstance(run_lists, str):
      run_lists = \
        base64.b64decode(
          run_lists.encode('ascii')).\
        decode('utf-8').\
        strip()
    if isinstance(run_lists, str):
      run_lists = \
        run_lists.split('\n')
    run_lists = [
      r.strip()
        for r in run_lists]
    # xcom has path to RTAComplete.txt
    run_lists = [
      os.path.basename(os.path.dirname(s))
        for s in run_lists]
    dbparam = \
      read_dbconf_json(DATABASE_CONFIG_FILE)
    sra = SeqrunAdaptor(**dbparam)
    new_seqruns = list()
    sra.start_session()
    for seqrun_id in run_lists:
      seqrun_exists = \
        sra.check_seqrun_exists(
          seqrun_id=seqrun_id)
      if not seqrun_exists:
        new_seqruns.\
          append(seqrun_id)
    sra.close_session()
    ## check if new seqruns are available
    if len(new_seqruns) == 0:
      return [no_work_task]
    else:
      ti.xcom_push(value=new_seqruns[0], key=seqrun_id_xcom_key)
      return [next_task]
  except Exception as e:
    log.error(e)
    message = \
      f'{e}, Log: {os.environ.get("AIRFLOW__LOGGING__BASE_LOG_FOLDER")}/dag_id={ti.dag_id}/run_id={ti.run_id}/task_id={ti.task_id}/attempt={ti.try_number}.log'
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise


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
    json_data = {
      "seqrun_id_list": [seqrun_id,]}
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
    message = \
      f'{e}, Log: {os.environ.get("AIRFLOW__LOGGING__BASE_LOG_FOLDER")}/dag_id={ti.dag_id}/run_id={ti.run_id}/task_id={ti.task_id}/attempt={ti.try_number}.log'
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise


def register_new_seqrun_to_db(
      dbconfig_file: str,
      seqrun_id: str,
      seqrun_base_path: str,
      runinfo_file_name: str = 'RunInfo.xml') \
        -> bool:
    try:
      check_file_path(dbconfig_file)
      runinfo_file_path = \
        os.path.join(
          seqrun_base_path,
          seqrun_id,
          runinfo_file_name)
      check_file_path(runinfo_file_path)
      runinfo_data = \
        RunInfo_xml(
          xml_file=runinfo_file_path)
      platform_id = \
        runinfo_data.\
          get_platform_number()
      flowcell_id = \
        runinfo_data.\
          get_flowcell_name()
      dbparam = \
        read_dbconf_json(dbconfig_file)
      pl = PlatformAdaptor(**dbparam)
      pl.start_session()
      sra = SeqrunAdaptor(**{'session': pl.session})
      seqrun_exists = \
        sra.check_seqrun_exists(
          seqrun_id=seqrun_id)
      ## only register if seqrun is not present in db
      ## and platform is known
      if seqrun_exists:
        ## no need to do anything
        pl.close_session()
        return False
      pl_record = \
        pl.fetch_platform_records_igf_id(
          platform_igf_id=platform_id,
          output_mode='one_or_none')
      if not seqrun_exists and \
         pl_record is not None:
        try:
          if (pl_record.model_name == 'HISEQ4000'):
            ## read info from runParameters.xml
            runparameters_file = \
              os.path.join(
                seqrun_base_path,
                seqrun_id,
                'runParameters.xml')
            check_file_path(runparameters_file)
            runparameters_data = \
              RunParameter_xml(
                xml_file=runparameters_file)
            flowcell = \
              runparameters_data.\
                get_hiseq_flowcell()
          elif (pl_record.model_name == 'NOVASEQ6000'):
            ## read info from RunParameters.xml
            runparameters_file = \
              os.path.join(
                seqrun_base_path,
                seqrun_id,
                'RunParameters.xml')
            check_file_path(runparameters_file)
            runparameters_data = \
              RunParameter_xml(
                xml_file=runparameters_file)
            flowcell = \
              runparameters_data.\
                get_novaseq_flowcell()
          else:
            flowcell = \
              pl_record.model_name
          seqrun_data = [{
            'seqrun_igf_id': seqrun_id,
            'flowcell_id': flowcell_id,
            'platform_igf_id': platform_id,
            'flowcell': flowcell}]
          sra.store_seqrun_and_attribute_data(
            data=seqrun_data)
        except:
          pl.rollback_session()
          pl.close_session()
          raise
      pl.close_session()
      return True
    except Exception as e:
      raise ValueError(
        f"Failed to register new run on db. error: {e}")