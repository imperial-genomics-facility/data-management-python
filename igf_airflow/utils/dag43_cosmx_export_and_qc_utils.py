import os
import re
import shutil
import logging
import subprocess
import pandas as pd
from pathlib import Path
from datetime import timedelta
from airflow.models import Variable
from yaml import load, SafeLoader
from igf_data.utils.bashutils import bash_script_wrapper
from igf_data.utils.jupyter_nbconvert_wrapper import Notebook_runner
from typing import (
  Any,
  List,
  Dict,
  Tuple,
  Union,
  Optional)
from igf_data.utils.fileutils import (
  check_file_path,
  copy_local_file,
  get_temp_dir,
  get_date_stamp)
from igf_airflow.utils.dag22_bclconvert_demult_utils import (
  _create_output_from_jinja_template)
from igf_airflow.utils.generic_airflow_utils import (
  get_project_igf_id_for_analysis,
  fetch_analysis_name_for_analysis_id,
  send_airflow_failed_logs_to_channels,
  send_airflow_pipeline_logs_to_channels,
  get_per_sample_analysis_groups,
  collect_analysis_dir,
  parse_analysis_design_and_get_metadata)
from igf_data.utils.cosmxutils import (
  check_and_register_cosmx_run,
  check_and_register_cosmx_slide,
  create_or_update_cosmx_slide_fov,
  create_or_update_cosmx_slide_fov_annotation,
  create_cosmx_slide_fov_count_qc,
  validate_cosmx_count_file)
from airflow.operators.python import get_current_context
from airflow.decorators import task

log = logging.getLogger(__name__)

## CONF
MS_TEAMS_CONF = \
  Variable.get(
      'analysis_ms_teams_conf', default_var=None)
DATABASE_CONFIG_FILE = \
  Variable.get('database_config_file', default_var=None)

## COSMX CONFIG
COSMX_EXPORT_DIR = \
  Variable.get('cosmx_export_base_dir', default_var='/TEST_EXPORT_DIR/')
COSMX_EXPORT_CONDA_ENV = \
  Variable.get('cosmx_export_conda_env', default_var='TEST_ENV')
COXMX_EXPORT_SCRIPT_PATH = \
  Variable.get('cosmx_export_script_path', default_var='TEST_SCRIPT')
COSMX_EXPORT_RCLONE_PROFILE = \
  Variable.get('cosmx_export_rclone_profile', default_var='TEST_PROFILE')


## TASKS
@task(multiple_outputs=False)
def run_ftp_export_factory(design_file: str, work_dir: str) -> List[Dict[str, str]]:
  try:
    with open(design_file, 'r') as fp:
      design_data = load(fp, Loader=SafeLoader)
    run_metadata = design_data.get("run_metadata")
    if not run_metadata:
      raise KeyError(
        f"Missing run_metadata in file {design_file}")
    if not isinstance(run_metadata, list):
      raise TypeError(f"Expecting a list of run_meatadata, received {type(run_metadata)}")
    return run_metadata
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))
    raise ValueError(e)


## TASK
@task(multiple_outputs=True)
def prepare_run_ftp_export(run_entry: Dict[str, str], work_dir: str) -> Dict[str, Any]:
  try:
    cosmx_ftp_export_name = run_entry.get("export_directory_path")
    if COSMX_EXPORT_DIR is not None and cosmx_ftp_export_name is not None:
        export_dir = os.path.join(COSMX_EXPORT_DIR, cosmx_ftp_export_name)
    else:
        raise KeyError('Missing COSMX_EXPORT_DIR or cosmx export_directory_path in for ftp transfer')
    run_entry.update({'work_dir': work_dir, 'export_dir': export_dir})
    return {'run_entry': run_entry, 'cosmx_ftp_export_name': cosmx_ftp_export_name}
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))
    raise ValueError(e)


## BASH TASK
@task.bash(retries=0)
def run_ftp_export(cosmx_ftp_export_name: str) -> str:
  try:
    bash_cmd = f"""set -eo pipefail;
    ## MOVE TO COSMX EXPORT DIR
    cd {COSMX_EXPORT_DIR};
    ## ACTIVATE CONDA ON HPC
    eval "$(~/anaconda3/bin/conda shell.bash hook)";
    conda activate {COSMX_EXPORT_CONDA_ENV};
    ## CHEKC AND REMOVE OLD EXPORT
    if [-d {cosmx_ftp_export_name} ]; then
      echo "Removing old export dir {cosmx_ftp_export_name}";
      rm -rf {cosmx_ftp_export_name};
    fi
    ## RUN EXPORT SCRIPT
    python {COXMX_EXPORT_SCRIPT_PATH} -r {COSMX_EXPORT_RCLONE_PROFILE} -q0 -s0 -f {cosmx_ftp_export_name}"""
    return bash_cmd
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))
    raise ValueError(e)


## TASK
@task(multiple_outputs=True)
def prep_extract_ftp_export(run_entry: Dict[str, str]) -> Dict[str, Any]:
  try:
    export_dir = run_entry.get("export_dir")
    if not export_dir:
      raise KeyError("Missing export_dir in run_entry")
    return {'run_entry': run_entry, 'export_dir': export_dir}
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))
    raise ValueError(e)


## BASH TASK
@task.bash(retries=0)
def extract_ftp_export(export_dir: str, work_dir: str) -> str:
  try:

    bash_cmd = \
      f"""set -eo pipefail;
        EXPORT_DIR={export_dir}
        EXPORT_DIR_NAME=$(basename $EXPORT_DIR)
        WORK_DIR={work_dir}
      """ + \
      """cd $WORK_DIR
        ## check for existing export dir and remove it
        if [ -d $EXPORT_DIR_NAME ]; then
            echo "Removing old export dir $EXPORT_DIR_NAME";
            rm -rf $EXPORT_DIR_NAME;
        fi
        ## create new export dir
        mkdir $EXPORT_DIR_NAME
        cd $EXPORT_DIR_NAME
        ## extract files
        RAWFILES_DIR="RawFiles"
        RAWFILES_ZIP="DecodedFiles.tar.gz"
        FLATFILES_DIR="FlatFiles"
        FLATFILES_ZIP="flatFiles.tar.gz"
        QC_DIR="QC"
        MD5SUM_DIR="md5sum"
        umask 077
        ## check if flatFiles.tar.gz is missing
        if [ ! -f $EXPORT_DIR/$FLATFILES_ZIP ]; then
            echo "Missing flatFiles.tar.gz"; exit 1;
        fi
        ## check if DecodedFiles.tar.gz is missing
        if [ ! -f $EXPORT_DIR/$RAWFILES_ZIP ]; then
          echo "Missing DecodedFiles.tar.gz"; exit 1;
        fi
        ## extract RawFiles
        if [ -d $RAWFILES_DIR ]; then
          rm -rf $RAWFILES_DIR;
        fi
        mkdir $RAWFILES_DIR;
        tar -C $RAWFILES_DIR -xzf $EXPORT_DIR/$RAWFILES_ZIP;
        find $RAWFILES_DIR -type d -exec chmod 700 {} \\;
        find $RAWFILES_DIR -type f -exec chmod 600 {} \\;
        ## extract FlatFiles
        if [ -d $FLATFILES_DIR ]; then
            rm -rf $FLATFILES_DIR;
        fi
        mkdir $FLATFILES_DIR
        tar -C $FLATFILES_DIR -xzf $EXPORT_DIR/$FLATFILES_ZIP
        find $FLATFILES_DIR -type d -exec chmod 700 {} \\;
        find $FLATFILES_DIR -type f -exec chmod 600 {} \\;
        ## create QC dir
        if [ -d $QC_DIR ]; then
          rm -rf $QC_DIR;
        fi
        mkdir $QC_DIR;
        cp -r $EXPORT_DIR/$MD5SUM_DIR .
        cp $EXPORT_DIR/*.RDS .;
        cp $EXPORT_DIR/TileDB.tar.gz ."""
    return bash_cmd
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))
    raise ValueError(e)


## TASK
@task(multiple_outputs=True)
def prep_validate_export_md5(run_entry: Dict[str, str]) -> Dict[str, Any]:
  try:
    export_dir = run_entry.get("export_dir")
    if not export_dir:
      raise KeyError("Missing export_dir in run_entry")
    return {'run_entry': run_entry, 'export_dir': export_dir}
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))
    raise ValueError(e)


## BASH TASK
@task.bash(retries=0)
def validate_export_md5(export_dir: str) -> str:
  try:
    bash_cmd = f"""set -eo pipefail;
      FLATFILE_DIR={export_dir}/FlatFiles
      """ + \
      """FLATFILES_MD5=../md5sum/md5sum_flatFiles.csv
      ## CHECK MD5
      cd $FLATFILE_DIR
      cat $FLATFILES_MD5 |awk -F',' '{print $1 " " $2}'|grep -v md5sum|md5sum -c"""
    return bash_cmd
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))
    raise ValueError(e)


## TASK
@task(multiple_outputs=False)
def collect_extracted_data(run_entry: Dict[str, str]) -> Dict[str, str]:
  try:
    ## TO DO: JUST A PLACE HOLDER FOR BASH TASK OUTPUT
    return run_entry
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))
    raise ValueError(e)


@task(multiple_outputs=False)
def collect_all_slides(run_entry_list: Union[List[Dict[str, str]], Any]) -> List[Dict[str, str]]:
  try:
    slide_data_list = list()
    for run_entry in run_entry_list:
      export_dir = run_entry.get("export_dir")
      cosmx_run_id = run_entry.get("cosmx_run_id")
      if not export_dir or not cosmx_run_id:
        raise KeyError(f"Missing export_dir or cosmx_run_id in run_entry: {run_entry}")
      ## CHECKING FLATFILES DIR
      flat_file_dir = Path(export_dir) / "FlatFiles"
      check_file_path(flat_file_dir)
      for slide_dir_name in os.listdir(flat_file_dir):
        slide_data_list.append({
          "cosmx_run_id": cosmx_run_id,
          "export_dir": export_dir,
          "slide_dir": slide_dir_name})
    return slide_data_list
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))

## TASK
@task(multiple_outputs=False)
def generate_count_qc_report(run_entry: Dict[str, str]) -> Dict[str, str]:
  try:
    count_qc_data = {}
    return count_qc_data
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))
    raise ValueError(e)


## TASK
@task(multiple_outputs=False)
def generate_fov_qc_report(run_entry: Dict[str, str]) -> Dict[str, str]:
  try:
    fov_qc_data = {}
    return fov_qc_data
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))
    raise ValueError(e)

## TASK
@task(multiple_outputs=False)
def generate_db_data(qc_list: List[Dict[str, str]]) -> Dict[str, str]:
  try:
    db_data = {}
    return db_data
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))
    raise ValueError(e)

## TASK
@task(multiple_outputs=False)
def copy_slide_data_to_globus(run_entry: Dict[str, str]) -> Dict[str, str]:
  try:
    globus_data = {}
    return globus_data
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))
    raise ValueError(e)


## TASK
@task(multiple_outputs=False)
def register_db_data(run_entry: Dict[str, str]) -> Dict[str, str]:
  try:
    registered_data = {}
    ## step 1: register cosmx run
    run_registration_status = \
      check_and_register_cosmx_run(
        project_igf_id='',
        cosmx_run_igf_id='',
        db_session_class=None)
    ## step 2: register cosmx slide
    slide_registration_status = \
      check_and_register_cosmx_slide(
        cosmx_run_igf_id='',
        cosmx_slide_igf_id='',
        cosmx_platform_igf_id='',
        panel_info='',
        assay_type='',
        version='',
        db_session_class=None,
        slide_metadata=[])
    ## step 3: fov registration
    fov_registration_status = \
      create_or_update_cosmx_slide_fov(
        cosmx_slide_igf_id='',
        fov_range='',
        slide_type='',
        db_session_class=None)
    ## step 4: fov count qc registration
    fov_count_registration_status = \
      create_cosmx_slide_fov_count_qc(
        cosmx_slide_igf_id='',
        fov_range='',
        slide_type='',
        db_session_class=None,
        slide_count_json_file='',
        rna_count_file_validation_schema='',
        protein_count_file_validation_schema='')
    return registered_data
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))
    raise ValueError(e)


## TASK
@task(multiple_outputs=False)
def collect_qc_reports_and_upload_to_portal(run_entry_list: Union[List[Dict[str, str]], Any]) -> Optional[bool]:
  try:
    return True
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))