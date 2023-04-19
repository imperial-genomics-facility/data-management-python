import os
import re
import json
import yaml
import time
import logging
import pandas as pd
from yaml import Loader, load
from typing import Tuple
from airflow.models import Variable
from yaml import Loader
from yaml import Dumper
from typing import Tuple
from typing import Union
from igf_data.igfdb.igfTables import Pipeline, Pipeline_seed, Project, Analysis
from igf_data.utils.fileutils import check_file_path, copy_local_file
from igf_data.utils.fileutils import get_temp_dir
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.igfdb.analysisadaptor import AnalysisAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_airflow.logging.upload_log_msg import send_log_to_channels
from igf_portal.api_utils import get_data_from_portal

log = logging.getLogger(__name__)

SLACK_CONF = Variable.get('slack_conf',default_var=None)
MS_TEAMS_CONF = Variable.get('ms_teams_conf',default_var=None)
HPC_SSH_KEY_FILE = Variable.get('hpc_ssh_key_file', default_var=None)
DATABASE_CONFIG_FILE = Variable.get('database_config_file', default_var=None)
HPC_BASE_RAW_DATA_PATH = Variable.get('hpc_base_raw_data_path', default_var=None)
IGF_PORTAL_CONF = Variable.get('igf_portal_conf', default_var=None)


def fetch_raw_analysis_queue_func(**context):
  try:
    ti = context["ti"]
    new_raw_analysis_list_key = \
      context['params'].\
        get("new_raw_analysis_list_key", "new_raw_analysis_list")
    new_raw_analysis = \
      get_data_from_portal(
        portal_config_file=IGF_PORTAL_CONF,
        url_suffix='/api/v1/raw_analysis/search_new_analysis')
    new_raw_analysis_list = \
      new_raw_analysis.get('new_analysis')
    ti.xcom_push(
        key=new_raw_analysis_list_key,
        value=new_raw_analysis_list)
  except Exception as e:
    log.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def process_raw_analysis_queue_func(**context):
  try:
    ti = context["ti"]
    new_raw_analysis_list_key = \
      context['params'].\
        get("new_raw_analysis_list_key", "new_raw_analysis_list")
    new_raw_analysis_list_task = \
      context['params'].\
        get("new_raw_analysis_list_task", "fetch_raw_analysis_queue")
    new_raw_analysis_list = \
      ti.xcom_pull(
        task_ids=new_raw_analysis_list_task,
        key=new_raw_analysis_list_key)
    if not isinstance(new_raw_analysis_list, list):
      raise TypeError(
        f"Expecting a list of raw analysis id and got {type(new_raw_analysis_list)}")
    for raw_analysis_id in new_raw_analysis_list:
      ## todo
      try:
        ## 1. fetch raw analysis data from portal
        (project_id, pipeline_id, analysis_name, analysis_yaml) = \
          fetch_raw_analysis_yaml_data(
            raw_analysis_id=raw_analysis_id,
            igf_portal_conf=IGF_PORTAL_CONF)
        ## 2. check and register new analysis on db
        analysis_reg = \
          check_and_register_new_analysis_data(
            project_id=project_id,
            pipeline_id=pipeline_id,
            analysis_name=analysis_name,
            analysis_yaml=analysis_yaml,
            dbconf_json=DATABASE_CONFIG_FILE)
        if analysis_reg:
          message = \
          f"Register new analysis: {analysis_name}"
        ## 3. check and trigger pipeline for new analysis
        pipeline_reg = \
          check_and_trigger_new_analysis(
            project_id=project_id,
            pipeline_id=pipeline_id,
            analysis_name=analysis_name,
            dbconf_json=DATABASE_CONFIG_FILE)
        if pipeline_reg:
          message += \
          f". Pipeline is ready: {analysis_name}"
        ## 4. mark raw analysis as synched on portal
        mark_project_synched_on_portal(
          raw_analysis_id=raw_analysis_id,
          igf_portal_conf=IGF_PORTAL_CONF)
        ## 5. send msg to channel
        send_log_to_channels(
          slack_conf=SLACK_CONF,
          ms_teams_conf=MS_TEAMS_CONF,
          task_id=context['task'].task_id,
          dag_id=context['task'].dag_id,
          comment=message,
          reaction='pass')
        time.sleep(10)
      except Exception as e:
        message = \
          f"Failed to register new analysis, error: {e}"
        log.error(message)
        send_log_to_channels(
          slack_conf=SLACK_CONF,
          ms_teams_conf=MS_TEAMS_CONF,
          task_id=context['task'].task_id,
          dag_id=context['task'].dag_id,
          comment=message,
          reaction='fail')
  except Exception as e:
    log.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def fetch_raw_analysis_yaml_data(
      raw_analysis_id: int,
      igf_portal_conf: str) -> Tuple[int, int, str, str]:
  try:
    raw_analysis_data = \
      get_data_from_portal(
        portal_config_file=igf_portal_conf,
        url_suffix=f'/api/v1/raw_analysis/get_raw_analysis_data/{raw_analysis_id}',
        request_mode='post')
    time.sleep(10)
    project_id = raw_analysis_data.get('project_id')
    pipeline_id = raw_analysis_data.get('pipeline_id')
    analysis_name = raw_analysis_data.get('analysis_name')
    analysis_yaml = raw_analysis_data.get('analysis_yaml')
    if project_id is None or \
       pipeline_id is None or \
       analysis_name is None or \
       analysis_yaml is None:
      raise KeyError(
        f"Missing required data in {raw_analysis_data}")
    return project_id, pipeline_id, analysis_name, analysis_yaml
  except Exception as e:
    raise ValueError(
      f"Failed to fetch raw analysis yaml data, error: {e}")

def mark_project_synched_on_portal(
      raw_analysis_id: int,
      igf_portal_conf: str) -> None:
  try:
    _ = \
      get_data_from_portal(
        portal_config_file=igf_portal_conf,
        url_suffix=f'/api/v1/raw_analysis/mark_analysis_synched/{raw_analysis_id}',
        request_mode='post')
    time.sleep(10)
  except Exception as e:
    raise ValueError(
      f"Failed to mark raw analysis synched on portal, error: {e}")


def check_and_register_new_analysis_data(
      project_id: int,
      pipeline_id: int,
      analysis_name: str,
      analysis_yaml: str,
      dbconf_json: str) -> bool:
  try:
    ## to do
    ## 1. check if project + analysis name exists
    ## 2. check pipeline exists
    ## 3. fetch pipeline name from pipeline id
    ## 4. register new analysis
    dbparams = \
      read_dbconf_json(dbconf_json)
    aa = AnalysisAdaptor(**dbparams)
    aa.start_session()
    pr = ProjectAdaptor(**{'session': aa.session})
    pl = PipelineAdaptor(**{'session': aa.session})
    analysis_id = \
      aa.check_analysis_record_by_analysis_name_and_project_id(
        analysis_name=analysis_name,
        project_id=project_id)
    if analysis_id is not None:
      return False
    else:
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
      ## fetch pipeline
      pipeline = \
        pl.fetch_pipeline_records_pipeline_name(
          pipeline_name=pipeline_id,
          target_column_name='pipeline_id',
          output_mode='one_or_none')
      if pipeline is None:
        raise ValueError(
          f'Failed to get any valid pipeline for id {pipeline_id}')
      analysis_type = pipeline.pipeline_name
      ## convert yaml to json
      analysis_json = \
        json.dumps(
          load(analysis_yaml, Loader=Loader))
      ## store new analysis
      data = [{
        "project_igf_id": project_igf_id,
        "analysis_type": analysis_type,
        "analysis_name": analysis_name,
        "analysis_description": analysis_json
      }]
      aa.store_analysis_data(data)
      aa.close_session()
      return True
  except Exception as e:
    raise ValueError(
      f"Failed to register raw analysis data, error: {e}")


def check_and_trigger_new_analysis(
      project_id: int,
      pipeline_id: int,
      analysis_name: str,
      dbconf_json: str) -> bool:
  try:
    ## to do
    ## 1. get analysis id using analysis_name and project id
    dbparams = \
      read_dbconf_json(dbconf_json)
    aa = AnalysisAdaptor(**dbparams)
    aa.start_session()
    pl = PipelineAdaptor(**{'session': aa.session})
    analysis_id = \
      aa.check_analysis_record_by_analysis_name_and_project_id(
        analysis_name=analysis_name,
        project_id=project_id)
    if analysis_id is None:
      raise ValueError(
        f"No analysis id found for {analysis_name}")
    else:
      ## unpack id
      (analysis_id,) = analysis_id
    ## fetch pipeline
    pipeline = \
      pl.fetch_pipeline_records_pipeline_name(
        pipeline_name=pipeline_id,
        target_column_name='pipeline_id',
        output_mode='one_or_none')
    if pipeline is None:
      raise ValueError(
        f'Failed to get any valid pipeline for id {pipeline_id}')
    ## 2. get pipeline seed status for analysis id and pipeline id
    pipe_seed = \
      pl.fetch_pipeline_seed(
        pipeline_id=pipeline_id,
        seed_id=analysis_id,
        seed_table='analysis',
        output_mode='one_or_none')
    ## 3. register new pipeline seed
    if pipe_seed is not None:
      return False
    else:
      data = [{
        'pipeline_id': pipeline_id,
        'seed_id': analysis_id,
        'seed_table': 'analysis'}]
      pl.create_pipeline_seed(data)
      return True
  except Exception as e:
    raise ValueError(
      f"Failed to trigger raw analysis pipeline, error: {e}")