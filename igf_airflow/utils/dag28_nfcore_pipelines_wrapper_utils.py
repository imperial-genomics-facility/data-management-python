import os
import re
import json
import yaml
import logging
import pandas as pd
from typing import Tuple
from airflow.models import Variable
from igf_data.utils.analysis_fastq_fetch_utils import get_fastq_and_run_for_samples
from yaml import Loader
from yaml import Dumper
from typing import Tuple
from typing import Union
from igf_data.igfdb.igfTables import Pipeline, Pipeline_seed, Project, Analysis
from igf_data.utils.fileutils import check_file_path, copy_local_file
from igf_data.utils.fileutils import get_temp_dir
from igf_data.utils.fileutils import get_date_stamp_for_file_name
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.igfdb.analysisadaptor import AnalysisAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_airflow.logging.upload_log_msg import send_log_to_channels
from igf_airflow.utils.dag22_bclconvert_demult_utils import _create_output_from_jinja_template
from igf_airflow.utils.dag26_snakemake_rnaseq_utils import fetch_analysis_design
from igf_airflow.utils.dag26_snakemake_rnaseq_utils import parse_analysis_design_and_get_metadata
from igf_airflow.utils.dag26_snakemake_rnaseq_utils import get_project_igf_id_for_analysis
from igf_nextflow.nextflow_utils.nextflow_input_formatter import prepare_input_for_multiple_nfcore_pipeline


log = logging.getLogger(__name__)

SLACK_CONF = Variable.get('slack_conf',default_var=None)
MS_TEAMS_CONF = Variable.get('ms_teams_conf',default_var=None)
HPC_SSH_KEY_FILE = Variable.get('hpc_ssh_key_file', default_var=None)
DATABASE_CONFIG_FILE = Variable.get('database_config_file', default_var=None)
HPC_BASE_RAW_DATA_PATH = Variable.get('hpc_base_raw_data_path', default_var=None)
IGF_PORTAL_CONF = Variable.get('igf_portal_conf', default_var=None)
HPC_FILE_LOCATION = Variable.get("hpc_file_location", default_var="HPC_PROJECT")

## NEXTFLOW
NEXTFLOW_RUNNER_TEMPLATE = Variable.get("nextflow_runner_template", default_var=None)
NEXTFLOW_CONF_TEMPLATE = Variable.get("nextflow_conf_template", default_var=None)

## EMAIL CONFIG
EMAIL_CONFIG = Variable.get("email_config", default_var=None)
EMAIL_TEMPLATE = Variable.get("seqrun_email_template", default_var=None)
DEFAULT_EMAIL_USER = Variable.get("default_email_user", default_var=None)

## GLOBUS
GLOBUS_ROOT_DIR = Variable.get("globus_root_dir", default_var=None)


def prepare_nfcore_pipeline_inputs(**context):
  try:
    ti = context["ti"]
    nextflow_command_key = \
      context['params'].\
      get("nextflow_command_key", "nextflow_command")
    nextflow_workdir_key = \
      context['params'].\
      get("nextflow_workdir_key", "nextflow_workdir")
    next_task = \
      context['params'].\
      get("next_task", "run_nfcore_pipeline")
    last_task = \
      context['params'].\
      get("last_task", "mark_analysis_seed_as_failed")
    required_analysis_metadata_key = \
      context['params'].\
      get("required_analysis_metadata_key",
          ['NXF_VER', 'nextflow_params', 'nfcore_pipeline'])
    ## dag_run.conf should have analysis_id
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
    ## get analysis design
    input_design_yaml = \
      fetch_analysis_design(
        analysis_id=analysis_id,
        pipeline_name=pipeline_name,
        dbconfig_file=DATABASE_CONFIG_FILE)
    ## get project name
    project_igf_id = \
      get_project_igf_id_for_analysis(
        analysis_id=analysis_id,
        dbconfig_file=DATABASE_CONFIG_FILE)
    ## prepare nextflow input files
    sample_metadata, analysis_metadata = \
      parse_analysis_design_and_get_metadata(
        input_design_yaml=input_design_yaml)
    if sample_metadata is None or \
       analysis_metadata is None:
        raise KeyError("Missing sample or analysis metadata")
    for key_name in required_analysis_metadata_key:
      if key_name not in analysis_metadata or \
         analysis_metadata.get(key_name) is None:
        raise KeyError(
          f"Missing required analysis metadata key {key_name}")
    work_dir = \
      get_temp_dir(use_ephemeral_space=True)
    nfcore_pipeline_name = \
      analysis_metadata.get('nfcore_pipeline')
    try:
      work_dir, runner_file = \
        prepare_input_for_multiple_nfcore_pipeline(
          runner_template_file=NEXTFLOW_RUNNER_TEMPLATE,
          config_template_file=NEXTFLOW_CONF_TEMPLATE,
          project_name=project_igf_id,
          hpc_data_dir=HPC_BASE_RAW_DATA_PATH,
          dbconf_file=DATABASE_CONFIG_FILE,
          sample_metadata=sample_metadata,
          analysis_metadata=analysis_metadata,
          nfcore_pipeline_name=nfcore_pipeline_name)
      ## push it ot xcom
      ti.xcom_push(
        key=nextflow_command_key,
        value=runner_file)
      ti.xcom_push(
        key=nextflow_workdir_key,
        value=work_dir)
      return [next_task,]
    except:
      return [last_task,]
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