import os,logging,subprocess,re,fnmatch
import pandas as pd
from copy import copy
from airflow.models import Variable
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.utils.fileutils import check_file_path
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.igfdb.analysisadaptor import AnalysisAdaptor
from igf_airflow.logging.upload_log_msg import send_log_to_channels
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
NEXTFLOW_SINGULARITY_CACHE_DIR = Variable.get('nextflow_singularity_cache_dir',default_var=None)


def change_pipeline_status(**context):
  pass

def copy_nf_atacseq_branch_func(**context):
  pass

def copy_data_to_irods_func(**context):
  pass

def copy_data_to_box_func(**context):
  pass

def run_nf_command_func(**context):
  try:
    nextflow_work_dir = ''
    nextflow_command = list()
    ti = context.get('ti')
    nextflow_command_xcom_task = \
      context['params'].get('nextflow_command_xcom_task')
    nextflow_command_xcom_key = \
      context['params'].get('nextflow_command_xcom_key')
    nextflow_work_dir_xcom_task = \
      context['params'].get('nextflow_work_dir_xcom_task')
    nextflow_work_dir_xcom_key = \
      context['params'].get('nextflow_work_dir_xcom_key')
    nextflow_work_dir = \
      ti.xcom_pull(
        task_ids=nextflow_work_dir_xcom_task,
        key=nextflow_work_dir_xcom_key)
    nextflow_command = \
      ti.xcom_pull(
        task_ids=nextflow_command_xcom_task,
        key=nextflow_command_xcom_key)
    check_file_path(nextflow_work_dir)
    if nextflow_command is None or \
       not isinstance(nextflow_command,list) or \
       len(nextflow_command)==0:
      raise ValueError('Failed to get command list for nextflow')
    message = \
      'Started Nextflow run, output path: {0}, command: {1}'.\
        format(nextflow_work_dir,' '.join(nextflow_command))
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='pass')
    subprocess.check_call(' '.join(nextflow_command),shell=True)
    message = \
      'Finished Nextflow run, output path: {0}, command: {1}'.\
        format(nextflow_work_dir,' '.join(nextflow_command))
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='pass')
  except Exception as e:
    message = \
      'Failed Nextflow run, workdir: {0}, cmd: {1}, error: {2}'.\
        format(nextflow_work_dir,' '.join(nextflow_command),e)
    logging.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise ValueError(e)


def prep_nf_run_func(**context):
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
        igenomes_base_path=IGENOME_BASE_PATH,
        nextflow_singularity_cache_dir=NEXTFLOW_SINGULARITY_CACHE_DIR)
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
      sample_igf_id_list = \
        _fetch_sample_ids_from_nextflow_analysis_design(
          analysis_description=analysis_description)                            # get list of sample_igf_ids from analysis description
      _check_sample_id_and_analysis_id_for_project(
        analysis_id=analysis_id,
        sample_igf_id_list=sample_igf_id_list,
        dbconfig_file=DATABASE_CONFIG_FILE)                                     # check if all the sample and analysis are linked to the same project or not
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


def _check_sample_id_and_analysis_id_for_project(
      analysis_id,sample_igf_id_list,dbconfig_file):
  '''
  An internal method for checking the consistency of sample ids and analysis records

  :param analysis_id: Analysis id
  :param sample_igf_id_list: A list of sample_igf_id
  :returns: None
  '''
  try:
    dbparams = read_dbconf_json(dbconfig_file)
    sa = SampleAdaptor(**dbparams)
    sa.start_session()
    project_igf_id_list = \
      sa.get_project_ids_for_list_of_samples(
        sample_igf_id_list=sample_igf_id_list)
    aa = AnalysisAdaptor(**{'session':sa.session})
    project_igf_id = \
      aa.fetch_project_igf_id_for_analysis_id(
        analysis_id=int(analysis_id))
    sa.close_session()
    if len(project_igf_id_list)>1:
      raise ValueError(
              'More than one project found for sample list, projects: {0}'.\
                format(project_igf_id_list))
    if len(project_igf_id_list)==1 and \
       project_igf_id_list[0]!=project_igf_id:
      raise ValueError(
              'Analysis is linked to project {0} and samples are linked to poject {1}'.\
                format(project_igf_id,project_igf_id_list))
  except Exception as e:
    raise ValueError(
            'Failed sample and project consistancy check, error: {0}'.\
              format(e))


def _fetch_sample_ids_from_nextflow_analysis_design(
      analysis_description,nextflow_design_key='nextflow_design'):
  '''
  An internal function for fetching sample igf ids from nextflow design

  :param analysis_description: A dictionary containing at least the following
    * key: nextflow_design_key
    * value: A list of dictionaries and each of the dictionaries should have a key sample_igf_id
  :param nextflow_design_key: A string for nextflow_design keyword, default nextflow_design
  :returns: A list of sample_igf_id
  '''
  try:
    sample_igf_id_list = list()
    if not isinstance(analysis_description,dict) or \
       nextflow_design_key not in analysis_description:
      raise TypeError(
              'Expecting a dictionary with key {0}'.\
                format(nextflow_design_key))
    nextflow_design = analysis_description.get(nextflow_design_key)
    if nextflow_design is None or \
       not isinstance(nextflow_design,list):
      raise ValueError(
              'Missing data for keyword {0}'.\
                format(nextflow_design_key))
    for entry in nextflow_design:
      if 'sample_igf_id' not in entry or \
         entry.get('sample_igf_id') is None:
        raise KeyError('No sample_igf_id found in the nextflow design')
      sample_igf_id_list.\
        append(entry.get('sample_igf_id') )
    if len(sample_igf_id_list)==0:
      raise ValueError('No sample igf id found for nextflow design')
    return sample_igf_id_list
  except Exception as e:
    raise ValueError(
            'Failed to get sample ids for analysis description: {0}, error: {1}'.\
              format(analysis_description,e))