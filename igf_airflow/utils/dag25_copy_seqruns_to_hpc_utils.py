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

log = logging.getLogger(__name__)

SLACK_CONF = Variable.get('slack_conf',default_var=None)
MS_TEAMS_CONF = Variable.get('ms_teams_conf',default_var=None)
DATABASE_CONFIG_FILE = Variable.get('database_config_file', default_var=None)
HPC_SEQRUN_PATH = Variable.get('hpc_seqrun_path', default_var=None)
IGF_PORTAL_CONF = Variable.get('igf_portal_conf', default_var=None)
PORTAL_ADD_SEQRUN_URL = "/api/v1/raw_seqrun/add_new_seqrun"
HPC_INTEROP_PATH = Variable.get('hpc_interop_path', default_var=None)
INTEROP_REPORT_TEMPLATE = Variable.get('interop_report_template', default_var=None)
INTEROP_REPORT_IMAGE = Variable.get('interop_report_image', default_var=None)

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


def  _create_interop_report(
    run_id: str,
    run_dir_base_path: str,
    report_template: str,
    report_image: str,
    timeout: int = 1200,
    no_input: bool = True,
    dry_run: bool = False,
    num_cpu: int = 8,
    ram_gb: int = 8,
    use_singularity_execute: bool = True,
    extra_container_dir_list: Optional[list] = None) -> \
      Tuple[str, str, str, str, str]:
  try:
    ## create a formated notebook using template
    ## * run id
    ## * run input dir
    ## * output dir
    ##
    work_dir = \
      get_temp_dir(use_ephemeral_space=True)
    os.makedirs(work_dir, exist_ok=True)
    run_dir = \
      os.path.join(run_dir_base_path, run_id)
    input_list = [
      report_template,
      report_image,
      work_dir,
      run_dir]
    for f in input_list:
      check_file_path(f)
    overview_csv_output = \
      os.path.join(
        work_dir,
        f"{run_id}_overview.csv")
    tile_parquet_output = \
      os.path.join(
        work_dir,
        f"{run_id}_tile.parquet")
    metrics_dir = \
      os.path.join(
          work_dir,
          'metrics')
    os.makedirs(metrics_dir, exist_ok=True)
    ##
    ## execute notebook on server and create report
    ## * input ipynb and output html
    ## * can we save ipynb
    ##
    container_bind_dir_list = [
      work_dir,
      run_dir]
    if extra_container_dir_list is not None and \
       len(extra_container_dir_list) > 0:
      container_bind_dir_list.extend(
        extra_container_dir_list)
    date_tag = get_date_stamp()
    input_params = dict(
      DATE_TAG=date_tag,
      RUN_ID=run_id,
      RUN_DIR=run_dir,
      METRICS_DIR=metrics_dir,
      OVERVIEW_CSV_OUTPUT=overview_csv_output,
      TILE_PARQUET_OUTOUT=tile_parquet_output,
      NUM_CPU=num_cpu,
      RAM_GB=ram_gb)
    nb = \
      Notebook_runner(
        template_ipynb_path=report_template,
        output_dir=work_dir,
        input_param_map=input_params,
        container_paths=container_bind_dir_list,
        kernel='python3',
        use_ephemeral_space=True,
        allow_errors=False,
        singularity_image_path=report_image,
        timeout=timeout,
        no_input=no_input,
        use_singularity_execute=use_singularity_execute,
        dry_run=dry_run)
    output_notebook_path, _ = \
      nb.execute_notebook_in_singularity()
    check_file_path(overview_csv_output)
    check_file_path(tile_parquet_output)
    check_file_path(output_notebook_path)
    return output_notebook_path, metrics_dir, overview_csv_output, tile_parquet_output, work_dir
  except Exception as e:
    raise ValueError(
      f"Failed to generate report, error: {e}")

def _load_interop_data_to_db(
      run_id: str,
      interop_output_dir: str,
      interop_report_base_path: str,
      dbconfig_file: str,
      collection_type: str = 'interop_metrics',
      collection_table: str = 'seqrun') -> None:
  try:
    date_stamp = \
      get_date_stamp_for_file_name()
    target_dir_path = \
      os.path.join(
        interop_report_base_path,
        f"{run_id}_interop_{date_stamp}")
    ## add fail safe
    if os.path.exists(target_dir_path):
      raise IOError(
        f"Path {target_dir_path} already present. Check and remove old files before re-run.")
    ## load data to disk
    copy_local_file(
      source_path=interop_output_dir,
      destination_path=target_dir_path)
    ## load analysis to db
    collection_data_list = [{
      'name': run_id,
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
  except Exception as e:
    raise ValueError(
      f"Failed to load interop report to db, error: {e}")

def _load_interop_overview_data_to_seqrun_attribute(
      seqrun_igf_id: str,
      dbconfig_file: str,
      interop_overview_file: str) -> None:
  try:
    check_file_path(interop_overview_file)
    overview_df = \
      pd.read_csv(interop_overview_file, header=0)
    attribute_list = list()
    for entry in overview_df.to_dict(orient='records'):
      for attribute_name, attribute_value in entry.items():
        attribute_list.append({
          'attribute_name': attribute_name,
          'attribute_value': attribute_value})
    dbparam = \
        read_dbconf_json(dbconfig_file)
    sra = SeqrunAdaptor(**dbparam)
    sra.start_session()
    try:
      sra.create_or_update_seqrun_attribute_records(
        seqrun_igf_id=seqrun_igf_id,
        attribute_list=attribute_list,
        autosave=False)
      sra.commit_session()
      sra.close_session()
    except:
      sra.rollback_session()
      sra.close_session()
      raise
  except Exception as e:
    raise ValueError(
      f"Failed to load overview data to seqrun attribute table, error: {e}")