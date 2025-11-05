from typing import Dict
import os
import json
import logging
from yaml import load, SafeLoader
from airflow.decorators import task
from datetime import timedelta
from airflow.models import Variable
from airflow.operators.python import get_current_context
from igf_data.utils.fileutils import (
  check_file_path,
  get_temp_dir)
from igf_portal.api_utils import get_data_from_portal
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.igfdb.analysisadaptor import AnalysisAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_airflow.utils.generic_airflow_utils import (
  send_airflow_failed_logs_to_channels,
  send_airflow_pipeline_logs_to_channels)

log = logging.getLogger(__name__)

## CONF
MS_TEAMS_CONF = \
  Variable.get(
      'analysis_ms_teams_conf', default_var=None)
DATABASE_CONFIG_FILE = \
  Variable.get('database_config_file', default_var=None)
IGF_PORTAL_CONF = \
  Variable.get('igf_portal_conf', default_var=None)
IGFPORTAL_RAW_ANALYSIS_SEARCH_URI = \
  '/api/v1/raw_analysis_v2/search_new_analysis'
IGFPORTAL_RAW_ANALYSIS_FETCH_URI = \
  '/api/v1/raw_analysis_v2/get_raw_analysis_data'
IGFPORTAL_RAW_ANALYSIS_SYNC_URI = \
  '/api/v1/raw_analysis_v2/mark_analysis_synched'


## TASK - find raw metadata id in datrun.conf
@task(
    task_id="find_raw_metadata_id",
    retry_delay=timedelta(minutes=5),
    retries=4,
    queue='hpc_4G')
def find_raw_metadata_id(
    raw_analysis_id_tag: str = "raw_analysis_id",
    dag_run_key: str = "dag_run") \
      -> int:
  try:
    ### dag_run.conf should have raw_analysis_id
    context = get_current_context()
    dag_run = context.get(dag_run_key)
    raw_analysis_id = None
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get(raw_analysis_id_tag) is not None:
      raw_analysis_id = \
        dag_run.conf.get(raw_analysis_id_tag)
    if raw_analysis_id is None:
      raise ValueError(
        'raw_analysis_id not found in dag_run.conf')
    return int(raw_analysis_id)
  except Exception as e:
    message = \
      f"Failed to get raw_analysis_id, error: {e}"
    log.error(message)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(message))
    raise ValueError(message)

## TASK - fetch raw analysis metadata from portal
@task(
    task_id="fetch_raw_metadata_from_portal",
    retries=1,
    queue='hpc_4G')
def fetch_raw_metadata_from_portal(
  raw_analysis_id: int) -> str:
  try:
    raw_analysis_data = \
      get_data_from_portal(
        portal_config_file=IGF_PORTAL_CONF,
        url_suffix=f'{IGFPORTAL_RAW_ANALYSIS_FETCH_URI}/{raw_analysis_id}',
        request_mode='get')
    project_id = raw_analysis_data.get('project_id')
    pipeline_id = raw_analysis_data.get('pipeline_id')
    analysis_name = raw_analysis_data.get('analysis_name')
    analysis_yaml = raw_analysis_data.get('analysis_yaml')
    if project_id is None or \
       pipeline_id is None or \
       analysis_name is None or \
       analysis_yaml is None:
      raise KeyError(
        f"Missing required data for raw analysis entry {raw_analysis_id}")
    temp_dir = get_temp_dir()
    raw_metadata_json_file = \
      os.path.join(temp_dir, "raw_metadata.json")
    with open(raw_metadata_json_file, "w") as fp:
      json.dump(raw_analysis_data, fp)
    return raw_metadata_json_file
  except Exception as e:
    message = \
      f"Failed to fetch raw_analysis_metadata, error: {e}"
    log.error(message)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(message))
    raise ValueError(message)


## FUNC
def check_registered_analysis_in_db(
      project_id: int,
      analysis_name: str,
      dbconf_json: str) -> bool:
  try:
    dbparams = \
      read_dbconf_json(dbconf_json)
    aa = AnalysisAdaptor(**dbparams)
    aa.start_session()
    analysis_id = \
      aa.check_analysis_record_by_analysis_name_and_project_id(
        analysis_name=analysis_name,
        project_id=project_id)
    aa.close_session()
    if analysis_id is not None:
      return False
    else:
      return True
  except Exception as e:
    raise ValueError(
      f"Failed to register raw analysis data, error: {e}")


## TASK - check raw metadata in db
@task(
    task_id="check_raw_metadata_in_db",
    retry_delay=timedelta(minutes=5),
    retries=4,
    queue='hpc_4G')
def check_raw_metadata_in_db(
  raw_metadata_file: str) \
    -> str:
  try:
    check_file_path(raw_metadata_file)
    with open(raw_metadata_file, "r") as fp:
      raw_analysis_data = json.load(fp)
    project_id = raw_analysis_data.get('project_id')
    pipeline_id = raw_analysis_data.get('pipeline_id')
    analysis_name = raw_analysis_data.get('analysis_name')
    analysis_yaml = raw_analysis_data.get('analysis_yaml')
    if project_id is None or \
       pipeline_id is None or \
       analysis_name is None or \
       analysis_yaml is None:
      raise KeyError(
        f"Missing required data for raw analysis entry file {raw_metadata_file}")
    analysis_reg = \
      check_registered_analysis_in_db(
        project_id=project_id,
        analysis_name=analysis_name,
        dbconf_json=DATABASE_CONFIG_FILE)
    if not analysis_reg:
      raw_metadata_file = ""
    return raw_metadata_file
  except Exception as e:
    message = \
      f"Failed to check existing raw_analysis_metadata, error: {e}"
    log.error(message)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(message))
    raise ValueError(message)

## FUNC
def register_analysis_in_db(
      project_id: int,
      pipeline_id: int,
      analysis_name: str,
      analysis_yaml: str,
      dbconf_json: str) -> bool:
  try:
    ## check another time is analysis is registered
    status = \
      check_registered_analysis_in_db(
        project_id=project_id,
        analysis_name=analysis_name,
        dbconf_json=dbconf_json)
    if not status:
      return False
    else:
      dbparams = \
        read_dbconf_json(dbconf_json)
      aa = AnalysisAdaptor(**dbparams)
      aa.start_session()
      pr = ProjectAdaptor(**{'session': aa.session})
      pl = PipelineAdaptor(**{'session': aa.session})
      ## fetch project
      project = \
        pr.fetch_project_records_igf_id(
        project_igf_id=project_id,
        target_column_name='project_id',
        output_mode='one_or_none')
      if project is None:
        raise ValueError(
          f'Failed to get any valid project for id {project_id}')
      project_igf_id = \
        project.project_igf_id
      pipeline = \
        pl.fetch_pipeline_records_pipeline_name(
          pipeline_name=pipeline_id,
          target_column_name='pipeline_id',
          output_mode='one_or_none')
      if pipeline is None:
        raise ValueError(
          f'Failed to get any valid pipeline for id {pipeline_id}')
      analysis_type = \
        pipeline.pipeline_name
      ## convert yaml to json
      if isinstance(analysis_yaml, str):
        analysis_json = \
          json.dumps(
            load(analysis_yaml, Loader=SafeLoader))
      elif isinstance(analysis_yaml, dict):
        analysis_json = \
          json.dumps(analysis_yaml)
      else:
        raise TypeError(
          "Expecting a yaml string or dictionary, " + \
          f"but got {type(analysis_yaml)}")
      ## store new analysis -
      data = [{
        "project_igf_id": project_igf_id,
        "analysis_type": analysis_type,
        "analysis_name": analysis_name,
        "analysis_description": analysis_json}]
      aa.store_analysis_data(data)
      ## fetch analysis id
      analysis_id = \
        aa.check_analysis_record_by_analysis_name_and_project_id(
          analysis_name=analysis_name,
          project_id=project_id,
          output_mode='one_or_none')
      if analysis_id is None:
        raise ValueError(
          f"No analysis id found for {analysis_name}")
      ## create pipeline seed status for analysis id and pipeline id
      data = [{
        'pipeline_id': pipeline_id,
        'seed_id': analysis_id[0],
        'seed_table': 'analysis'}]
      pl.create_pipeline_seed(data)
      aa.close_session()
      return True
  except Exception as e:
    raise ValueError(
      f"Failed to register raw analysis data, error: {e}")


## TASK - register raw metadata in db
@task(
    task_id="register_raw_metadata_in_db",
    retry_delay=timedelta(minutes=5),
    retries=4,
    queue='hpc_4G',
    multiple_outputs=False)
def register_raw_analysis_metadata_in_db(valid_raw_metadata_file: str) -> bool:
  try:
    if valid_raw_metadata_file == "":
      return False
    else:
      check_file_path(valid_raw_metadata_file)
      with open(valid_raw_metadata_file, "r") as fp:
        raw_analysis_data = json.load(fp)
      project_id = raw_analysis_data.get('project_id')
      pipeline_id = raw_analysis_data.get('pipeline_id')
      analysis_name = raw_analysis_data.get('analysis_name')
      analysis_yaml = raw_analysis_data.get('analysis_yaml')
      if project_id is None or \
         pipeline_id is None or \
         analysis_name is None or \
         analysis_yaml is None:
        raise KeyError(
          f"Missing required data for raw analysis entry file {valid_raw_metadata_file}")
      status = \
        register_analysis_in_db(
        project_id=project_id,
        pipeline_id=pipeline_id,
        analysis_name=analysis_name,
        analysis_yaml=analysis_yaml,
        dbconf_json=DATABASE_CONFIG_FILE)
    return status
  except Exception as e:
    message = \
      f"Failed to register raw_analysis_metadata, error: {e}"
    log.error(message)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(message))
    raise ValueError(message)

## TASK - mark raw metadata as synced on portal
@task(
    task_id="mark_metadata_synced_on_portal",
    retry_delay=timedelta(minutes=5),
    retries=4,
    queue='hpc_4G')
def mark_metadata_synced_on_portal(
  raw_analysis_id: int,
  registration_status: bool) -> None:
  try:
    if registration_status:
      _ = \
        get_data_from_portal(
          portal_config_file=IGF_PORTAL_CONF,
          url_suffix=f'/api/v1/raw_analysis_v2/mark_analysis_synched/{raw_analysis_id}',
          request_mode='post')
      message = \
        f"Registration is successful for raw analysis {raw_analysis_id}"
      send_airflow_pipeline_logs_to_channels(
        ms_teams_conf=MS_TEAMS_CONF,
        message_prefix=str(message))
    else:
      message = \
        f"Registration is not successful for raw analysis {raw_analysis_id}"
      send_airflow_failed_logs_to_channels(
        ms_teams_conf=MS_TEAMS_CONF,
        message_prefix=str(message))
  except Exception as e:
    message = \
      f"Failed to mark raw analysis as synced on portal, error: {e}"
    log.error(message)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(message))
    raise ValueError(message)