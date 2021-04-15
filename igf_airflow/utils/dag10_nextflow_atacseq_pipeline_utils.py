import os,logging,subprocess,re,fnmatch
import pandas as pd
from copy import copy
from airflow.models import Variable
from igf_nextflow.nextflow_utils.nextflow_runner import nextflow_pre_run_setup
from igf_airflow.utils.dag9_tenx_single_cell_immune_profiling_utils import _check_and_mark_analysis_seed

DATABASE_CONFIG_FILE = Variable.get('test_database_config_file',default_var=None)
SLACK_CONF = Variable.get('analysis_slack_conf',default_var=None)
MS_TEAMS_CONF = Variable.get('analysis_ms_teams_conf',default_var=None)
ASANA_CONF = Variable.get('asana_conf',default_var=None)
ASANA_PROJECT = Variable.get('asana_analysis_project',default_var=None)
BOX_USERNAME = Variable.get('box_username',default_var=None)
BOX_CONFIG_FILE = Variable.get('box_config_file',default_var=None)
IRDOS_EXE_DIR = Variable.get('irods_exe_dir',default_var=None)
BOX_DIR_PREFIX = 'SecondaryAnalysis'
IGENOME_BASE_PATH = Variable.get('igenome_base_path',default_var=None)
NEXTFLOW_TEMPLATE_FILE = Variable.get('nextflow_template_file',default_var=None)
NEXTFLOW_EXE = Variable.get('nextflow_exe',default_var=None)


def change_pipeline_status(**context):
  pass

def run_nf_atacseq_func(**context):
  pass

def copy_nf_atacseq_branch_func(**context):
  pass

def copy_data_to_irods_func(**context):
  pass

def copy_data_to_box_func(**context):
  pass

def prep_nf_atacseq_run_func(**context):
  try:
    ti = context.get('ti')
    analysis_description_xcom_key = \
      context['params'].get('analysis_description_xcom_key')
    analysis_description_xcom_task = \
      context['params'].get('analysis_description_xcom_task')
    nextflow_command_xcom_key = \
      context['params'].get('nextflow_command_xcom_key')
    nextflow_work_dir_xcom_key = \
      context['params'].get('nextflow_work_dir_xcom_key')
    analysis_description = \
      ti.xcom_pull(
        task_ids=analysis_description_xcom_task,
        key=analysis_description_xcom_key)
    nextflow_command_list,nextflow_work_dir = \
      nextflow_pre_run_setup(
        nextflow_exe=NEXTFLOW_EXE,
        analysis_description=analysis_description,
        dbconf_file=DATABASE_CONFIG_FILE,
        nextflow_config_template=NEXTFLOW_TEMPLATE_FILE,
        igenomes_base_path=IGENOME_BASE_PATH)
    ti.xcom_push(
      key=nextflow_command_xcom_key,
      value=nextflow_command_list)
    ti.xcom_push(
      key=nextflow_work_dir_xcom_key,
      value=nextflow_work_dir)
  except Exception as e:
    logging.error(e)
    raise ValueError(e)

def fetch_nextflow_analysis_info_and_branch_func(**context):
  try:
    dag_run = context.get('dag_run')
    ti = context.get('ti')
    no_analysis = \
      context['params'].get('no_analysis_task')
    active_tasks = \
      context['params'].get('active_tasks')
    if not isinstance(active_tasks,list):
      raise TypeError('Expecting a list of active tasks')
    analysis_description_xcom_key = \
      context['params'].get('analysis_description_xcom_key')
    analysis_list = [no_analysis]
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('analysis_description') is not None:
      analysis_description = \
        dag_run.conf.get('analysis_description')
      analysis_id = \
        dag_run.conf.get('analysis_id')
      analysis_type = \
        dag_run.conf.get('analysis_type')
      status = \
        _check_and_mark_analysis_seed(
          analysis_id=analysis_id,
          anslysis_type=analysis_type,
          new_status='RUNNING',
          no_change_status='RUNNING',
          database_config_file=DATABASE_CONFIG_FILE)
      if status:
        analysis_list = active_tasks
        ti.xcom_push(
          key=analysis_description_xcom_key,
          value=analysis_description)
    return analysis_list
  except Exception as e:
    logging.error(e)
    raise ValueError(e)