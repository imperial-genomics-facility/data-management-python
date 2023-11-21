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
from igf_airflow.utils.dag22_bclconvert_demult_utils import (
    _create_output_from_jinja_template,
    send_email_via_smtp)
from igf_airflow.utils.dag26_snakemake_rnaseq_utils import (
    fetch_analysis_design,
    parse_analysis_design_and_get_metadata,
    get_project_igf_id_for_analysis,
    calculate_analysis_name,
    load_analysis_and_build_collection,
    copy_analysis_to_globus_dir,
    check_and_seed_analysis_pipeline)
from airflow.operators.python import get_current_context
from airflow.decorators import task

log = logging.getLogger(__name__)

SLACK_CONF = Variable.get('analysis_slack_conf',default_var=None)
MS_TEAMS_CONF = Variable.get('analysis_ms_teams_conf',default_var=None)
HPC_SSH_KEY_FILE = Variable.get('hpc_ssh_key_file', default_var=None)
DATABASE_CONFIG_FILE = Variable.get('database_config_file', default_var=None)
HPC_BASE_RAW_DATA_PATH = Variable.get('hpc_base_raw_data_path', default_var=None)
HPC_FILE_LOCATION = Variable.get("hpc_file_location", default_var="HPC_PROJECT")

## EMAIL CONFIG
EMAIL_CONFIG = Variable.get("email_config", default_var=None)
EMAIL_TEMPLATE = Variable.get("seqrun_email_template", default_var=None)
DEFAULT_EMAIL_USER = Variable.get("default_email_user", default_var=None)

## GLOBUS
GLOBUS_ROOT_DIR = Variable.get("globus_root_dir", default_var=None)

## EMAIL CONFIG
EMAIL_CONFIG = Variable.get("email_config", default_var=None)
EMAIL_TEMPLATE = Variable.get("analysis_email_template", default_var=None)

## CELLRANGER
CELLRANGER_SCRIPT_TEMPLATE = \
  Variable.get("cellranger_run_script_template", default_var=None)
## TASK
@task(
  task_id="get_analysis_group_list",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def get_analysis_group_list(design_dict: dict) -> dict:
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
      raise KeyError("Missing sample or analysis metadata")
    unique_sample_groups = set()
    for _, group in sample_metadata.items():
      grp_name = group.get('cellranger_group')
      if grp_name is None:
        raise KeyError("Missing cellranger_group in sample_metadata")
      unique_sample_groups.add(grp_name)
    if len(unique_sample_groups) == 0:
      raise ValueError("No sample group found")
    return list(unique_sample_groups)
  except Exception as e:
    context = get_current_context()
    log.error(e)
    log_file_path = [
      os.environ.get('AIRFLOW__LOGGING__BASE_LOG_FOLDER'),
      f"dag_id={context['ti'].dag_id}",
      f"run_id={context['ti'].run_id}",
      f"task_id={context['ti'].task_id}",
      f"attempt={context['ti'].try_number}.log"]
    message = \
      f"Error: {e}, Log: {os.path.join(*log_file_path)}"
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      project_id=None,
      comment=message,
      reaction='fail')
    raise ValueError(e)


## TASK
@task(
  task_id="prepare_cellranger_script",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def prepare_cellranger_script(sample_group: str, design_dict: dict) -> dict:
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
      raise KeyError("Missing sample or analysis metadata")
    work_dir = get_temp_dir(use_ephemeral_space=True)
    library_csv_file, run_script_file = \
      prepare_cellranger_run_dir_and_script_file(
        sample_group=str(sample_group),
        work_dir=work_dir,
        design_file=design_file,
        db_config_file=DATABASE_CONFIG_FILE,
        run_script_template=CELLRANGER_SCRIPT_TEMPLATE)
    return {"sample_group": sample_group, "run_script": run_script_file, "run_dir": work_dir}
  except Exception as e:
    context = get_current_context()
    log.error(e)
    log_file_path = [
      os.environ.get('AIRFLOW__LOGGING__BASE_LOG_FOLDER'),
      f"dag_id={context['ti'].dag_id}",
      f"run_id={context['ti'].run_id}",
      f"task_id={context['ti'].task_id}",
      f"attempt={context['ti'].try_number}.log"]
    message = \
      f"Error: {e}, Log: {os.path.join(*log_file_path)}"
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      project_id=None,
      comment=message,
      reaction='fail')
    raise ValueError(e)

def prepare_cellranger_run_dir_and_script_file(
      sample_group: str,
      work_dir: str,
      design_file: str,
      db_config_file: str,
      run_script_template: str,
      library_csv_filename: str = 'library.csv') \
        -> str:
  try:
    check_file_path(design_file)
    check_file_path(work_dir)
    check_file_path(run_script_template)
    with open(design_file, 'r') as fp:
      input_design_yaml=fp.read()
    sample_metadata, analysis_metadata = \
      parse_analysis_design_and_get_metadata(
        input_design_yaml=input_design_yaml)
    if sample_metadata is None or \
       analysis_metadata is None:
      raise KeyError("Missing sample or analysis metadata")
    ## library info
    sample_library_list = \
      create_library_information_for_sample_group(
        sample_group=sample_group,
        sample_metadata=sample_metadata,
        db_config_file=db_config_file)
    ## get cellranger conf
    cellranger_multi_config = \
      analysis_metadata.get("cellranger_multi_config")
    if cellranger_multi_config is None:
      raise KeyError("Missing cellranger_multi_config in analysis design")
    ## create temp dir and dump script and library.csv
    library_csv_file = \
      os.path.join(
        work_dir,
        library_csv_filename)
    sample_library_csv = \
      pd.DataFrame(sample_library_list).\
      to_csv(index=False)
    with open(library_csv_file, 'w') as fp:
      fp.write('\n'.join(cellranger_multi_config))
      fp.write('\n') ## add an empty line
      fp.write('[libraries]\n')
      fp.write(sample_library_csv)
    ## create run script from template
    script_file = \
      os.path.join(
        work_dir,
        os.path.basename(run_script_template))
    _create_output_from_jinja_template(
      template_file=run_script_template,
      output_file=script_file,
      autoescape_list=['xml',],
      data=dict(
        CELLRANGER_MULTI_ID=str(sample_group),
        CELLRANGER_MULTI_CSV=library_csv_file,
        CELLRANGER_MULTI_OUTPUT_DIR=work_dir,
        WORKDIR=work_dir))
    return library_csv_file, script_file
  except Exception as e:
    raise ValueError(
      f"Failed to prepare cellranger script, error: {e}")


def create_library_information_for_sample_group(
      sample_group: str,
      sample_metadata: dict,
      db_config_file: str) -> list:
  try:
    ## get cellranger group
    sample_group_dict = dict()
    sample_igf_id_list = list()
    for sample_igf_id, group in sample_metadata.items():
      grp_name = group.get('cellranger_group')
      feature_types = group.get('feature_types')
      if grp_name is None or feature_types is None:
        raise KeyError(
          "Missing cellranger_group or feature_types in sample_metadata ")
      if str(grp_name) == str(sample_group):
        sample_igf_id_list.append(sample_igf_id)
      sample_group_dict.update({ sample_igf_id: feature_types})
    ## get sample ids from metadata
    if len(sample_igf_id_list) == 0:
      raise ValueError("No sample id found in the metadata")
    ## get fastq files for all samples
    fastq_list = \
      get_fastq_and_run_for_samples(
        dbconfig_file=db_config_file,
        sample_igf_id_list=sample_igf_id_list)
    if len(fastq_list) == 0:
      raise ValueError(
        "No fastq file found for samples")
    ## create libraries section
    df = pd.DataFrame(fastq_list)
    sample_library_list = list()
    for _, g_data in df.groupby(['sample_igf_id', 'run_igf_id', 'flowcell_id', 'lane_number']):
      sample_igf_id = g_data['sample_igf_id'].values[0]
      fastq_file_path = g_data['file_path'].values[0]
      fastq_dir = os.path.dirname(fastq_file_path)
      feature_types = sample_group_dict.get(sample_igf_id)
      if feature_types is None:
        raise KeyError(
          f"No feature_types found for sample {sample_igf_id}")
      fastq_id = \
        os.path.basename(fastq_file_path).split("_")[0]
      sample_library_list.append({
        "fastq_id": fastq_id,
        "fastqs": fastq_dir,
        "feature_types": feature_types})
    return sample_library_list
  except Exception as e:
    raise ValueError(
      f"Failed to prepare cellranger script, error: {e}")
