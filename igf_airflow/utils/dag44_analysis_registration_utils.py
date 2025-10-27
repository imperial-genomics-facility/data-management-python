from typing import (
  Any,
  List,
  Dict,
  Tuple,
  Union,
  Optional)
import json
import time
import shutil
import logging
import subprocess
from pathlib import Path
from yaml import load, SafeLoader
from airflow.decorators import task
from datetime import timedelta, datetime
from airflow.models import Variable
from igf_portal.api_utils import get_data_from_portal
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.igfdb.analysisadaptor import AnalysisAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_airflow.utils.generic_airflow_utils import (
  get_project_igf_id_for_analysis,
  get_project_igf_id_for_analysis,
  fetch_analysis_name_for_analysis_id,
  copy_analysis_to_globus_dir,
  send_airflow_failed_logs_to_channels,
  send_airflow_pipeline_logs_to_channels,
  get_per_sample_analysis_groups,
  collect_analysis_dir,
  parse_analysis_design_and_get_metadata,
  get_analysis_id_and_project_igf_id_from_airflow_dagrun_conf)

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
    queue='hpc_4G',
    multiple_outputs=False)
def find_raw_metadata_id():
    return {"raw_metadata_id": 1}

## TASK - fetch raw analysis metadata from portal
@task(
    task_id="fetch_raw_metadata_from_portal",
    retry_delay=timedelta(minutes=5),
    retries=4,
    queue='hpc_4G',
    multiple_outputs=False)
def fetch_raw_metadata_from_portal(raw_metadata_id):
    return {"raw_metadata_file": "raw_metadata.json"}

## TASK - check raw metadata in db
@task(
    task_id="check_raw_metadata_in_db",
    retry_delay=timedelta(minutes=5),
    retries=4,
    queue='hpc_4G',
    multiple_outputs=False)
def check_raw_metadata_in_db(raw_metadata_file):
    return {"valid_raw_metadata_file": "raw_metadata.json"}

## TASK - register raw metadata in db
@task(
    task_id="register_raw_metadata_in_db",
    retry_delay=timedelta(minutes=5),
    retries=4,
    queue='hpc_4G',
    multiple_outputs=False)
def register_raw_metadata_in_db(valid_raw_metadata_file):
    return {"status": True}

## TASK - mark raw metadata as synced on portal
@task(
    task_id="mark_metadata_synced_on_portal",
    retry_delay=timedelta(minutes=5),
    retries=4,
    queue='hpc_4G',
    multiple_outputs=False)
def mark_metadata_synced_on_portal(raw_metadata_id, registration_status):
    return {"status": True}