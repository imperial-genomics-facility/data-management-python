import os
import re
import json
import yaml
import shutil
import zipfile
import logging
import pandas as pd
from typing import (
    Tuple,
    Optional)
from datetime import timedelta
from airflow.models import Variable
from igf_data.utils.bashutils import bash_script_wrapper
from igf_data.utils.analysis_fastq_fetch_utils import get_fastq_and_run_for_samples
from igf_data.utils.jupyter_nbconvert_wrapper import Notebook_runner
from jinja2 import Template
from yaml import (
    Loader,
    Dumper)
from typing import (
    Tuple,
    Union)
from igf_data.igfdb.igfTables import (
    Pipeline,
    Pipeline_seed,
    Project,
    Analysis)
from igf_data.utils.fileutils import (
    check_file_path,
    copy_local_file,
    get_temp_dir,
    read_json_data,
    get_date_stamp,
    get_date_stamp_for_file_name)
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.igfdb.analysisadaptor import AnalysisAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_airflow.logging.upload_log_msg import send_log_to_channels
from igf_airflow.utils.dag22_bclconvert_demult_utils import _create_output_from_jinja_template
from airflow.operators.python import get_current_context
from airflow.decorators import task
from igf_airflow.utils.generic_airflow_utils import (
    get_project_igf_id_for_analysis,
    fetch_analysis_yaml_and_dump_to_a_file,
    fetch_analysis_name_for_analysis_id,
    get_fastq_for_samples_and_dump_in_json_file,
    send_airflow_failed_logs_to_channels,
    collect_analysis_dir,
    copy_analysis_to_globus_dir,
    calculate_md5sum_for_analysis_dir,
    fetch_user_info_for_project_igf_id,
    generate_email_text_for_analysis,
    parse_analysis_design_and_get_metadata
)


log = logging.getLogger(__name__)

SLACK_CONF = Variable.get('analysis_slack_conf',default_var=None)
MS_TEAMS_CONF = Variable.get('analysis_ms_teams_conf',default_var=None)
HPC_SSH_KEY_FILE = Variable.get('hpc_ssh_key_file', default_var=None)
DATABASE_CONFIG_FILE = Variable.get('database_config_file', default_var=None)
HPC_BASE_RAW_DATA_PATH = Variable.get('hpc_base_raw_data_path', default_var=None)
IGF_PORTAL_CONF = Variable.get('igf_portal_conf', default_var=None)
HPC_FILE_LOCATION = Variable.get("hpc_file_location", default_var="HPC_PROJECT")

## GLOBUS
GLOBUS_ROOT_DIR = Variable.get("globus_root_dir", default_var=None)

## GEOMX CONF VARIABLES
GEOMX_NGS_PIPELINE_EXE = Variable.get("geomx_ngs_pipeline_exe", default_var=None)
GEOMX_SCRIPT_TEMPLATE = Variable.get("geomx_script_template", default_var=None)
REPORT_TEMPLATE_FILE = Variable.get("geomx_report_template_file", default_var=None)
REPORT_IMAGE_FILE = Variable.get("geomx_report_image_file", default_var=None)


# ## TASK
# @task(
#   task_id="fetch_analysis_design",
#   retry_delay=timedelta(minutes=5),
#   retries=4,
#   queue='hpc_4G')
# def fetch_analysis_design_from_db() -> dict:
#   try:
#     ## dag_run.conf should have analysis_id
#     context = get_current_context()
#     dag_run = context.get('dag_run')
#     analysis_id = None
#     if dag_run is not None and \
#        dag_run.conf is not None and \
#        dag_run.conf.get('analysis_id') is not None:
#       analysis_id = \
#         dag_run.conf.get('analysis_id')
#     if analysis_id is None:
#       raise ValueError(
#         'analysis_id not found in dag_run.conf')
#     ## pipeline_name is context['task'].dag_id
#     pipeline_name = context['task'].dag_id
#     ## get analysis design file
#     temp_yaml_file = \
#       fetch_analysis_yaml_and_dump_to_a_file(
#         analysis_id=analysis_id,
#         pipeline_name=pipeline_name,
#         dbconfig_file=DATABASE_CONFIG_FILE)
#     return {'analysis_design': temp_yaml_file}
#   except Exception as e:
#     log.error(e)
#     send_airflow_failed_logs_to_channels(
#       slack_conf=SLACK_CONF,
#       ms_teams_conf=MS_TEAMS_CONF,
#       message_prefix=e)
#     raise ValueError(e)


def extract_geomx_config_files_from_zip(zip_file: str) -> Tuple[str, str]:
  try:
    check_file_path(zip_file)
    ## get temp dir and copy zip file to it
    temp_dir = get_temp_dir(use_ephemeral_space=True)
    temp_zip_file = os.path.join(temp_dir, 'geomx_config.zip')
    ## get extract dir
    extract_dir = os.path.join(temp_dir, 'extract')
    os.makedirs(extract_dir)
    ## copy zip file to temp dir
    shutil.copy2(zip_file, temp_zip_file)
    ## extract zip file
    with zipfile.ZipFile(temp_zip_file, 'r') as zip_ref:
      zip_ref.extractall(extract_dir)
      # config_file = list()
      # labworksheet_file = list()
      config_file = None
      labworksheet_file = None
      for root, _, files in os.walk(extract_dir):
        for f in files:
          if f.endswith('.ini'):
            config_file = \
              os.path.join(root, f)
          if f.endswith('LabWorksheet.txt'):
            labworksheet_file = \
              os.path.join(root, f)
    if config_file is None or \
       labworksheet_file is None:
      raise ValueError(
        f"No LabWorksheet.txt or config.ini file present in {zip_file}")
    new_config_file = \
      os.path.join(extract_dir, 'geomx_project.ini')
    new_labworksheet_file = \
      os.path.join(extract_dir, 'geomx_project_LabWorksheet.txt')
    if os.path.exists(new_config_file) or \
    os.path.exists(new_labworksheet_file):
      raise IOError(
        f"{new_config_file} or {new_labworksheet_file} file is already present")
    shutil.copy2(config_file, new_config_file)
    shutil.copy2(labworksheet_file, new_labworksheet_file)
    return new_config_file, new_labworksheet_file
  except Exception as e:
    raise ValueError(
      f"Failed to get config file, error: {e}")


## TASK
@task(
  task_id="check_and_process_config_file",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def check_and_process_config_file(design_dict: dict) -> dict:
  try:
    design_file = design_dict.get('analysis_design')
    check_file_path(design_file)
    with open(design_file, 'r') as fp:
      input_design_yaml = fp.read()
    sample_metadata, analysis_metadata = \
      parse_analysis_design_and_get_metadata(
        input_design_yaml=input_design_yaml)
    if sample_metadata is None or \
       analysis_metadata is None:
      raise KeyError(
        "Missing sample or analysis metadata")
    config_zip_file = \
      analysis_metadata.get('config_zip_file')
    check_file_path(config_zip_file)
    config_ini_file = None
    labworksheet_file = None
    config_ini_file, labworksheet_file = \
      extract_geomx_config_files_from_zip(
        zip_file=config_zip_file)
    if config_ini_file is None or \
      labworksheet_file is None:
        raise ValueError(
          f"Missing ini or worksheet in {config_zip_file}")
    config_file_dict = {
      'config_ini_file': config_ini_file,
      'labworksheet_file': labworksheet_file}
    return config_file_dict
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


## TASK
@task(
  task_id="fetch_fastq_file_path_from_db",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def fetch_fastq_file_path_from_db(design_dict: dict) -> str:
  try:
    design_file = design_dict.get('analysis_design')
    fastq_list_json = \
      get_fastq_for_samples_and_dump_in_json_file(
        design_file=design_file,
        db_config_file=DATABASE_CONFIG_FILE)
    return fastq_list_json
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


def read_fastq_list_json_and_create_symlink_dir_for_geomx_ngs(
      fastq_list_json: str,
      required_columns: tuple = (
        'sample_igf_id',
        'run_igf_id',
        'flowcell_id',
        'lane_number',
        'file_path') ) -> str:
  try:
    check_file_path(fastq_list_json)
    df = pd.read_json(fastq_list_json) ## use pydantic??
    ## check for required columns
    for c in required_columns:
      if c not in df.columns:
        raise KeyError(
          f"Missing required column {c} in json file {fastq_list_json}")
    df['file_name'] = \
      df['file_path'].map(lambda x: os.path.basename(x))
    duplicate_rows = \
      len(df[df['file_name'].duplicated()].index)
    ## get symlink path
    symlink_dir = get_temp_dir(use_ephemeral_space=True)
    symlink_dict = dict()
    pattern = \
      re.compile(r'(\S+)_S(\d+)_(L00\d)_(R[1,2])_001.fastq.gz')
    if duplicate_rows > 0:
      ## make filenames unique
      counter = 999
      for _, g_data in df.groupby(['sample_igf_id', 'run_igf_id', 'flowcell_id', 'lane_number']):
        counter += 1
        for r in g_data.to_dict(orient='records'):
          source_path = r['file_path']
          source_name = r['file_name']
          match = re.match(pattern, source_name)
          if match:
            (sample_id, s_id, l_id, r_id) = match.groups()
            dest_name = f"{sample_id}_S{counter}_{l_id}_{r_id}_001.fastq.gz"
            dest_path = os.path.join(symlink_dir, dest_name)
            symlink_dict.update({source_path: dest_path})
    else:
      for r in df[['file_path', 'file_name']].to_dict(orient='records'):
        source_path = r['file_path']
        dest_name = r['file_name']
        dest_path = os.path.join(symlink_dir, dest_name)
        match = re.match(pattern, dest_name)
        if match:
          symlink_dict.update({source_path: dest_path})
    ## create symlink
    if len(symlink_dict) == 0:
      raise ValueError(
        f"No entry found for symlink creation. json: {fastq_list_json}")
    for source_path, dest_path in symlink_dict.items():
      os.symlink(source_path, dest_path)
    return symlink_dir
  except Exception as e:
    raise ValueError(
      f"Failed to create symlink dir, error: {e}")


## TASK
@task(
  task_id="create_temp_fastq_input_dir",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def create_temp_fastq_input_dir(fastq_list_json: str) -> str:
  try:
    symlink_dir = \
      read_fastq_list_json_and_create_symlink_dir_for_geomx_ngs(fastq_list_json)
    return symlink_dir
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


def create_sample_translation_file_for_geomx_script(
      design_file: str,
      dsp_id_key: str = 'dsp_id') -> str:
  try:
    ## check design file
    check_file_path(design_file)
    with open(design_file, 'r') as fp:
      input_design_yaml = fp.read()
    sample_metadata, analysis_metadata = \
      parse_analysis_design_and_get_metadata(
        input_design_yaml=input_design_yaml)
    if sample_metadata is None or \
       analysis_metadata is None:
      raise KeyError(
        "Missing sample or analysis metadata")
    translation_file_data = list()
    for sample_id, sample_data in sample_metadata.items():
      dsp_id = sample_data.get(dsp_id_key)
      if dsp_id is None:
        raise KeyError(
          f"Missing DSP id for sample {sample_id} in {design_file}")
      translation_file_data.append({
        'sample_id': sample_id,
        'dsp_id': dsp_id})
    temp_translation_dir = \
      get_temp_dir(use_ephemeral_space=True)
    translation_file = \
      os.path.join(temp_translation_dir, "translation.csv")
    df = pd.DataFrame(translation_file_data)
    df[['dsp_id', 'sample_id']].\
      to_csv(
        translation_file,
        index=False,
        header=False)
    return translation_file
  except Exception as e:
    raise ValueError(
      f"Failed to create translation file, error: {e}")


def fetch_geomx_params_from_analysis_design(
      design_file: str,
      param_key: str = 'geomx_dcc_params') -> list:
  try:
    ## check design file
    check_file_path(design_file)
    with open(design_file, 'r') as fp:
      input_design_yaml = fp.read()
    sample_metadata, analysis_metadata = \
      parse_analysis_design_and_get_metadata(
        input_design_yaml=input_design_yaml)
    if sample_metadata is None or \
       analysis_metadata is None:
      raise KeyError(
        "Missing sample or analysis metadata")
    params_list = list()
    if param_key in analysis_metadata:
      params_list = \
        analysis_metadata.get(param_key)
    if not isinstance(params_list, list):
      raise TypeError(f"Expection a list of params, got {params_list}")
    return params_list
  except Exception as e:
    raise ValueError(
      f"Failed to get Geomx ngs pipeline params, error: {e}")


def create_geomx_dcc_run_script(
      geomx_script_template: str,
      geomx_ngs_pipeline_exe: str,
      design_file: str,
      symlink_dir: str,
      config_file_dict: dict) -> Tuple[str, str]:
  try:
    ## check input files
    ## check template file
    check_file_path(geomx_script_template)
    ## check exe file
    check_file_path(geomx_ngs_pipeline_exe)
    ## check design file
    check_file_path(design_file)
    ## check symlink dir
    check_file_path(symlink_dir)
    ## create translation file for run
    translation_file = \
      create_sample_translation_file_for_geomx_script(
        design_file=design_file)
    check_file_path(translation_file)
    ## get ini config file
    config_ini_file = \
      config_file_dict.\
        get('config_ini_file')
    check_file_path(config_ini_file)
    ## fetch params list
    geomx_params_list = \
      fetch_geomx_params_from_analysis_design(
        design_file=design_file)
    ## generate rendered template
    work_dir = \
      get_temp_dir(use_ephemeral_space=True)
    output_dir = \
      get_temp_dir(use_ephemeral_space=True)
    ## temp dir path is getting copied every where. adding a simple name
    output_dir = \
      os.path.join(output_dir, 'geomx_dcc_counts')
    os.makedirs(output_dir, exist_ok=True)
    script_file = \
      os.path.join(work_dir, 'geomx_ngs_script.sh')
    _create_output_from_jinja_template(
      template_file=geomx_script_template,
      output_file=script_file,
      autoescape_list=['html',],
      data=dict(
        WORK_DIR=work_dir,
        GEOMX_NGS_EXE=geomx_ngs_pipeline_exe,
        FASTQ_DIR=symlink_dir,
        OUTPUT_DIR=output_dir,
        INPUT_INI_FILE=config_ini_file,
        INPUT_TRANSLATION_FILE=translation_file,
        GEOMX_PARAMS=geomx_params_list))
    return script_file, output_dir
  except Exception as e:
    raise ValueError(
      f"Failed to create dcc run script, error: {e}")


## TASK
@task(
  task_id="prepare_geomx_dcc_run_script",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def prepare_geomx_dcc_run_script(
      design_dict: dict,
      symlink_dir: str,
      config_file_dict: dict) -> Tuple[str, str]:
  try:
    design_file = design_dict.get('analysis_design')
    dcc_script_path, output_dir = \
      create_geomx_dcc_run_script(
        geomx_script_template=GEOMX_SCRIPT_TEMPLATE,
        geomx_ngs_pipeline_exe=GEOMX_NGS_PIPELINE_EXE,
        design_file=design_file,
        symlink_dir=symlink_dir,
        config_file_dict=config_file_dict)
    return {'dcc_script_path': dcc_script_path, 'output_dir': output_dir}
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


def compare_dcc_output_dir_with_design_file(
      dcc_output_dir: str,
      design_file: str) -> None:
  try:
    ## check dcc output file path
    dcc_files = [
      f for f in os.listdir(dcc_output_dir)
        if f.endswith('.dcc')]
    if len(dcc_files) == 0:
      raise ValueError(
        f"No dcc file found in path {dcc_output_dir}")
    ## check design file
    check_file_path(design_file)
    with open(design_file, 'r') as fp:
      input_design_yaml = fp.read()
    sample_metadata, analysis_metadata = \
      parse_analysis_design_and_get_metadata(
        input_design_yaml=input_design_yaml)
    if sample_metadata is None or \
      analysis_metadata is None:
        raise KeyError("Missing sample or analysis metadata")
    sample_list = list(sample_metadata.keys())
    ## tool generates dcc file for sample if its present in the .ini
    ## but not listed in the fastq directory
    if len(dcc_files) < len(sample_list):
      raise ValueError(
        f"""DCC file count: {len(dcc_files)},
        sample count: {len(sample_list)},
        path: {dcc_output_dir}""")
  except Exception as e:
    raise ValueError(
      f"Failed to check dcc files: {e}")


## TASK
@task(
  task_id="generate_dcc_count",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_64G16t')
def generate_geomx_dcc_count(
      design_dict: dict,
      dcc_script_dict: dict) -> str:
  try:
    script_path = dcc_script_dict.get('dcc_script_path')
    output_path = dcc_script_dict.get('output_dir')
    design_file = design_dict.get('analysis_design')
    ## its not safe to assume that files can be overwritten without inspection
    if len(os.listdir(output_path)) > 0:
      raise ValueError(
        f"Output path {output_path} is not empty. Clean-up manually before re-run.")
    try:
      stdout_file, stderr_file = \
        bash_script_wrapper(
          script_path=script_path)
    except Exception as e:
      raise ValueError(
        f"Failed to run script, Script: {script_path}, error file: {e}")
    ## check output path and compare dcc files
    ## with the number of samples mentioned in the design file
    compare_dcc_output_dir_with_design_file(
      dcc_output_dir=output_path,
      design_file=design_file)
    return output_path
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)

## TASK
@task(
  task_id="copy_geomx_config_file_to_output",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def copy_geomx_config_file_to_output(
      design_dict: dict,
      dcc_count_path: str) -> str:
  try:
    design_file = design_dict.get('analysis_design')
    check_file_path(design_file)
    with open(design_file, 'r') as fp:
      input_design_yaml = fp.read()
    sample_metadata, analysis_metadata = \
      parse_analysis_design_and_get_metadata(
        input_design_yaml=input_design_yaml)
    if sample_metadata is None or \
       analysis_metadata is None:
      raise KeyError(
        "Missing sample or analysis metadata")
    config_zip_file = \
      analysis_metadata.get('config_zip_file')
    check_file_path(config_zip_file)
    target_path = \
      os.path.join(
        dcc_count_path,
        'geomx_config.zip')
    if os.path.exists(target_path):
      raise IOError(
        f"Config file {target_path} already exists. Remove it before restarting pipeline.")
    copy_local_file(
      config_zip_file,
      target_path)
    return target_path
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


## TASK
@task(
  task_id="generate_qc_report",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_8G')
def generate_geomx_qc_report(
      dcc_count_path: str,
      config_file_dict: dict,
      design_dict: dict) -> str:
  try:
    design_file = design_dict.get('analysis_design')
    check_file_path(design_file)
    with open(design_file, 'r') as fp:
      input_design_yaml = fp.read()
    sample_metadata, analysis_metadata = \
      parse_analysis_design_and_get_metadata(
        input_design_yaml=input_design_yaml)
    if sample_metadata is None or \
       analysis_metadata is None:
      raise KeyError(
        "Missing sample or analysis metadata")
    geomx_pkc_file = \
      analysis_metadata.get('geomx_pkc_file')
    labworksheet_file = \
      config_file_dict.\
      get('labworksheet_file')
    ## get output dir
    output_path = \
      os.path.join(dcc_count_path, 'geomx_qc_report')
    os.makedirs(output_path, exist_ok=True)
    ## dag_run.conf should have analysis_id
    context = get_current_context()
    dag_run = context.get('dag_run')
    analysis_id = None
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('analysis_id') is not None:
      analysis_id = \
        dag_run.conf.get('analysis_id')
    if analysis_id is None:
      raise ValueError(
        'analysis_id not found in dag_run.conf')
    ## get analysis name and project name
    project_igf_id = \
      get_project_igf_id_for_analysis(
        analysis_id=analysis_id,
        dbconfig_file=DATABASE_CONFIG_FILE)
    analysis_name = \
      fetch_analysis_name_for_analysis_id(
        analysis_id=analysis_id,
        dbconfig_file=DATABASE_CONFIG_FILE)
    ## build report
    output_notebook = \
    build_qc_report_for_geomx(
      project_igf_id=project_igf_id,
      analysis_name=analysis_name,
      report_template=REPORT_TEMPLATE_FILE,
      image_file=REPORT_IMAGE_FILE,
      dcc_dir_path=dcc_count_path,
      pkc_file_path=geomx_pkc_file,
      annotation_file_path=labworksheet_file)
    ## copy report to output dir
    target_path = \
      os.path.join(
        output_path,
        os.path.basename(output_notebook))
    copy_local_file(
      output_notebook,
      target_path)
    return target_path
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


def build_qc_report_for_geomx(
      project_igf_id: str,
      analysis_name: str,
      report_template: str,
      image_file: str,
      dcc_dir_path: str,
      pkc_file_path: str,
      annotation_file_path: str,
      no_input: bool = True,
      timeout: int = 1200) -> str:
  try:
    work_dir = \
      get_temp_dir(use_ephemeral_space=True)
    input_list = [
      report_template,
      image_file,
      dcc_dir_path,
      pkc_file_path,
      annotation_file_path]
    for f in input_list:
      check_file_path(f)
      container_bind_dir_list = [
        dcc_dir_path,
        os.path.dirname(pkc_file_path),
        os.path.dirname(annotation_file_path)]
    date_tag = get_date_stamp()
    input_params = dict(
      DATE_TAG=date_tag,
      PROJECT_IGF_ID=project_igf_id,
      ANALYSIS_NAME=analysis_name,
      GEOMX_DCC_DIR=dcc_dir_path,
      GEOMX_ANNOTATION_FILE=annotation_file_path,
      GEOMX_PKC_FILE=pkc_file_path)
    nb = \
      Notebook_runner(
        template_ipynb_path=report_template,
        output_dir=work_dir,
        input_param_map=input_params,
        container_paths=container_bind_dir_list,
        kernel='python3',
        use_ephemeral_space=True,
        singularity_options=['-C'],
        allow_errors=False,
        singularity_image_path=image_file,
        timeout=timeout,
        no_input=no_input)
    output_notebook_path, _ = \
      nb.execute_notebook_in_singularity()
    output_notebook = \
      os.path.join(
        work_dir,
        f"{project_igf_id}_{os.path.basename(output_notebook_path)}")
    copy_local_file(
      output_notebook_path,
      output_notebook,
      force=True)
    return output_notebook
  except Exception as e:
    raise ValueError(
      f"Failed to generate qc report. Error: {e}")


## TASK
@task(
  task_id="calculate_md5sum_for_dcc",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_8G')
def calculate_md5sum_for_dcc(dcc_count_path: str) -> str:
  try:
    md5_sum_file = \
      calculate_md5sum_for_analysis_dir(
        dir_path=dcc_count_path)
    return md5_sum_file
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


## TASK
@task(
  task_id="load_dcc_count_to_db",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def load_dcc_count_to_db(
      dcc_count_path: str,
      md5_file: str,
      report_file: str,
      geomx_config: str) -> str:
  try:
    ## dag_run.conf should have analysis_id
    context = get_current_context()
    dag_run = context.get('dag_run')
    analysis_id = None
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('analysis_id') is not None:
      analysis_id = \
        dag_run.conf.get('analysis_id')
    if analysis_id is None:
      raise ValueError(
        'analysis_id not found in dag_run.conf')
    ## check if path exists
    check_file_path(geomx_config)
    check_file_path(md5_file)
    check_file_path(report_file)
    ## load data to db
    ## pipeline_name is context['task'].dag_id
    pipeline_name = context['task'].dag_id
    target_dir_path, project_igf_id, date_tag = \
      collect_analysis_dir(
        analysis_id=analysis_id,
        dag_name=pipeline_name,
        dir_path=dcc_count_path,
        db_config_file=DATABASE_CONFIG_FILE,
        hpc_base_path=HPC_BASE_RAW_DATA_PATH)
    return {'target_dir_path': target_dir_path, 'date_tag': date_tag}
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)