import os
import re
import json
import shutil
import logging
import subprocess
import pandas as pd
from pathlib import Path
from datetime import timedelta, datetime
from airflow.models import Variable
from yaml import load, SafeLoader
from dateutil.tz import gettz
from dateutil.parser import parse
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
  get_project_igf_id_for_analysis,
  fetch_analysis_name_for_analysis_id,
  send_airflow_failed_logs_to_channels,
  send_airflow_pipeline_logs_to_channels,
  get_per_sample_analysis_groups,
  collect_analysis_dir,
  parse_analysis_design_and_get_metadata,
  get_analysis_id_and_project_igf_id_from_airflow_dagrun_conf)
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
COSMX_QC_REPORT_IMAGE1 = \
  Variable.get('cosmx_qc_report_image1', default_var='TEST_IMAGE')
COSMX_SLIDE_METADATA_EXTRACTION_TEMPLATE = \
  Variable.get('cosmx_slide_metadata_extraction_template', default_var='TEST_TEMPLATE')
COSMX_COUNT_QC_REPORT_TEMPLATE = \
  Variable.get('cosmx_count_qc_report_template', default_var='TEST_TEMPLATE')
COSMX_COUNT_FOV_REPORT_TEMPLATE = \
  Variable.get('cosmx_count_fov_report_template', default_var='TEST_TEMPLATE')


## TASKS
@task(multiple_outputs=False)
def run_ftp_export_factory(design_file: str, work_dir: str) -> List[Dict[str, str]]:
  try:
    with open(design_file, 'r') as fp:
      design_data = load(fp, Loader=SafeLoader)
    run_metadata = design_data.get("run_metadata")
    ## check run entry
    for entry in run_metadata:
      if "cosmx_run_id" not in entry or \
         "export_directory_path" not in entry:
        raise KeyError(
          f"Check design file {design_file} for missing \
            cosmx_run_id or export_directory_path")
    if not run_metadata:
      raise KeyError(
        f"Missing run_metadata in file {design_file}")
    if not isinstance(run_metadata, list):
      raise TypeError(
        f"Expecting a list of run_meatadata, \
          received {type(run_metadata)}")
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
def prep_extract_ftp_export(
  run_entry: Dict[str, str],
  export_finished: Any) \
    -> Dict[str, Any]:
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
def prep_validate_export_md5(
  run_entry: Dict[str, str],
  extract_finished: Any) \
    -> Dict[str, Any]:
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
def collect_extracted_data(
  run_entry: Dict[str, str],
  validation_finished: Any) \
    -> Dict[str, str]:
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
          "slide_id": slide_dir_name})
    return slide_data_list
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))


## TASK
@task(multiple_outputs=False)
def match_slide_ids_with_project_id(
  slide_data_list: List[Dict[str, str]]) -> bool:
  """
  A function for checking if slides are linked to correct projects

  :param slide_data_list: A list of dictionaries containing 
      * cosmx_run_id
      * export_dir
      * slide_id
  :returns: True if all slide matches
  :raises:
    * ValueError if slide ids are different from project id
    * KeyError if slide_id is not present in the slide_data_list elements
  """
  try:
    ## step 1: get analysis id
    ### dag_run.conf should have analysis_id
    # context = get_current_context()
    # dag_run = context.get('dag_run')
    # analysis_id = None
    # if dag_run is not None and \
    #    dag_run.conf is not None and \
    #    dag_run.conf.get('analysis_id') is not None:
    #   analysis_id = \
    #     dag_run.conf.get('analysis_id')
    # if analysis_id is None:
    #   raise ValueError(
    #     'analysis_id not found in dag_run.conf')
    ## step 2: get project id of analysis
    # project_igf_id = \
    #   get_project_igf_id_for_analysis(
    #     analysis_id=analysis_id,
    #     dbconfig_file=DATABASE_CONFIG_FILE)
    analysis_id, project_igf_id = \
      get_analysis_id_and_project_igf_id_from_airflow_dagrun_conf(
        database_config_file=DATABASE_CONFIG_FILE)
    ## step 3: get slide ids and check if slide ids have same prefix as project ids
    for entry in slide_data_list:
      slide_id = entry.get("slide_id")
      if slide_id is None:
        raise KeyError(
          "Missing slide_id in slide_data_list")
      if project_igf_id not in slide_id:
        raise ValueError(
          f"Slide id {slide_id} not matching project id {project_igf_id}")
    return True
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))
    raise ValueError(e)


## TASK
@task(multiple_outputs=False)
def collect_slide_metadata(
  slide_entry: Dict[str, str],
  matched_slide_ids: Any,
  raw_files_dir_name: str = 'DecodedFiles',
  flat_files_dir_name: str = 'FlatFiles',
  metadata_json_key:str = "slide_metadata_json",
  metadata_json_file_name: str = 'slide_metadata.json') \
    -> Dict[str, str]:
  try:
    ## step 1: get analysis id
    ### dag_run.conf should have analysis_id
    # context = get_current_context()
    # dag_run = context.get('dag_run')
    # analysis_id = None
    # if dag_run is not None and \
    #    dag_run.conf is not None and \
    #    dag_run.conf.get('analysis_id') is not None:
    #   analysis_id = \
    #     dag_run.conf.get('analysis_id')
    # if analysis_id is None:
    #   raise ValueError(
    #     'analysis_id not found in dag_run.conf')
    ## step 2: get project id of analysis
    # project_igf_id = \
    #   get_project_igf_id_for_analysis(
    #     analysis_id=analysis_id,
    #     dbconfig_file=DATABASE_CONFIG_FILE)
    analysis_id, project_igf_id = \
      get_analysis_id_and_project_igf_id_from_airflow_dagrun_conf(
        database_config_file=DATABASE_CONFIG_FILE)
    ## step 3: get slide_id
    slide_id = slide_entry.get("slide_id")
    if slide_id is None:
      raise KeyError(
        "Missing slide_id in slide_entry")
    cosmx_run_id = slide_entry.get("cosmx_run_id")
    if cosmx_run_id is None:
      raise KeyError(
        "Missing cosmx_run_id in slide_entry")
    ## step 4: collect rawfiles dir
    export_dir = slide_entry.get("export_dir")
    if export_dir is None:
      raise KeyError(
        "Missing export_dir in slide_entry")
    raw_files_dir = Path(export_dir) / raw_files_dir_name
    ## step 5: collect flatfiles dir
    flat_files_dir = Path(export_dir) / flat_files_dir_name
    ## step 6: json output path
    temp_dir = get_temp_dir(use_ephemeral_space=True)
    metadata_json_file = Path(temp_dir) / metadata_json_file_name
    ## step 7: run notebook and generate json file
    input_list = [
      COSMX_SLIDE_METADATA_EXTRACTION_TEMPLATE,
      COSMX_QC_REPORT_IMAGE1,
      raw_files_dir,
      flat_files_dir,
      temp_dir]
    for f in input_list:
      check_file_path(f)
    container_bind_dir_list = [
      export_dir,
      temp_dir]
    date_tag = get_date_stamp()
    input_params = dict(
      DATE_TAG=date_tag,
      COSMX_PROJECT_NAME=project_igf_id,
      COSMX_SLIDE_NAME=slide_id,
      SLIDE_RAW_FILES_DIR=raw_files_dir,
      SLIDE_FLAT_FILES_DIR=flat_files_dir,
      JSON_OUTPUT_PATH=metadata_json_file)
    nb = \
      Notebook_runner(
        template_ipynb_path=COSMX_SLIDE_METADATA_EXTRACTION_TEMPLATE,
        output_dir=temp_dir,
        input_param_map=input_params,
        container_paths=container_bind_dir_list,
        kernel='python3',
        use_ephemeral_space=True,
        singularity_options=['-C'],
        allow_errors=False,
        singularity_image_path=COSMX_QC_REPORT_IMAGE1,
        timeout=120,
        no_input=False)
    _, _ = \
      nb.execute_notebook_in_singularity() ## no need to copy notebook file as we just need the json output
    ## step 8: check json file
    check_file_path(metadata_json_file)
    ## step 9: update slide_entry dict
    new_slide_entry = {
      "cosmx_run_id": cosmx_run_id,
      "slide_id": slide_id,
      "export_dir": export_dir,
      metadata_json_key: metadata_json_file,
      "flatfiles_dir": flat_files_dir}
    return new_slide_entry
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))


## TASK
@task(multiple_outputs=False)
def generate_count_qc_report(
  slide_entry: Dict[str, str],
  flat_files_dir_name: str = 'FlatFiles',
  report_files_dir_name: str = 'Reports',
  metadata_json_key:str = "slide_metadata_json",) -> Dict[str, str]:
  try:
    new_slide_entry = {}
    ## step 1: get analysis id
    ### dag_run.conf should have analysis_id
    # context = get_current_context()
    # dag_run = context.get('dag_run')
    # analysis_id = None
    # if dag_run is not None and \
    #    dag_run.conf is not None and \
    #    dag_run.conf.get('analysis_id') is not None:
    #   analysis_id = \
    #     dag_run.conf.get('analysis_id')
    # if analysis_id is None:
    #   raise ValueError(
    #     'analysis_id not found in dag_run.conf')
    # ## step 2: get project id of analysis
    # project_igf_id = \
    #   get_project_igf_id_for_analysis(
    #     analysis_id=analysis_id,
    #     dbconfig_file=DATABASE_CONFIG_FILE)
    analysis_id, project_igf_id = \
      get_analysis_id_and_project_igf_id_from_airflow_dagrun_conf(
        database_config_file=DATABASE_CONFIG_FILE)
    ## step 3: get slide_id
    slide_id = slide_entry.get("slide_id")
    if slide_id is None:
      raise KeyError(
        "Missing slide_id in slide_entry")
    cosmx_run_id = slide_entry.get("cosmx_run_id")
    if cosmx_run_id is None:
      raise KeyError(
        "Missing cosmx_run_id in slide_entry")
    ## step 4: collect flatfiles dir
    export_dir = slide_entry.get("export_dir")
    if export_dir is None:
      raise KeyError(
        "Missing export_dir in slide_entry")
    flat_files_dir = Path(export_dir) / flat_files_dir_name
    ## step 5: collect metadata json path
    metadata_json_file = slide_entry.get(metadata_json_key)
    ## step 6: run notebook and generate report
    temp_dir = get_temp_dir(use_ephemeral_space=True)
    input_list = [
      COSMX_COUNT_QC_REPORT_TEMPLATE,
      COSMX_QC_REPORT_IMAGE1,
      flat_files_dir,
      temp_dir]
    for f in input_list:
      check_file_path(f)
    container_bind_dir_list = [
      export_dir,
      temp_dir]
    date_tag = get_date_stamp()
    input_params = dict(
      DATE_TAG=date_tag,
      COSMX_PROJECT_NAME=project_igf_id,
      COSMX_SLIDE_NAME=slide_id,
      SLIDE_FLAT_FILE_DIR=flat_files_dir,
      SLIDE_METADATA_JSON_FILE=metadata_json_file,
      JSON_OUTPUT_DIR=temp_dir)
    nb = \
      Notebook_runner(
        template_ipynb_path=COSMX_COUNT_QC_REPORT_TEMPLATE,
        output_dir=temp_dir,
        input_param_map=input_params,
        container_paths=container_bind_dir_list,
        kernel='python3',
        use_ephemeral_space=True,
        singularity_options=['-C'],
        allow_errors=False,
        singularity_image_path=COSMX_QC_REPORT_IMAGE1,
        timeout=120,
        no_input=False)
    output_notebook, _ = \
      nb.execute_notebook_in_singularity()
    ## step 7: copy report to reports dir
    report_dir = Path(export_dir) / report_files_dir_name
    os.makedirs(report_dir, exist_ok=True)
    target_notebook_path = \
      report_dir / os.path.basename(output_notebook)
    copy_local_file(
      output_notebook, target_notebook_path.as_posix())
    ## step 8: return new slide entry
    new_slide_entry = {
      "cosmx_run_id": cosmx_run_id,
      "slide_id": slide_id,
      "export_dir": export_dir,
      metadata_json_key: metadata_json_file,
      "json_output": temp_dir,
      "flatfiles_dir": flat_files_dir}
    return new_slide_entry
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))
    raise ValueError(e)


## TASK
@task(multiple_outputs=False)
def generate_fov_qc_report(
  slide_entry: Dict[str, str],
  flat_files_dir_name: str = 'FlatFiles',
  report_files_dir_name: str = 'Reports',
  metadata_json_key:str = "slide_metadata_json",
  panel_name_key: str = 'panel_name'
) -> Dict[str, str]:
  try:
    new_slide_entry = {}
    ## step 1: get analysis id
    ## step 2: get project id of analysis
    analysis_id, project_igf_id = \
      get_analysis_id_and_project_igf_id_from_airflow_dagrun_conf(
        database_config_file=DATABASE_CONFIG_FILE)
    ## step 3: get slide_id
    slide_id = slide_entry.get("slide_id")
    if slide_id is None:
      raise KeyError(
        "Missing slide_id in slide_entry")
    cosmx_run_id = slide_entry.get("cosmx_run_id")
    if cosmx_run_id is None:
      raise KeyError(
        "Missing cosmx_run_id in slide_entry")
    ## step 4: collect flatfiles dir
    export_dir = slide_entry.get("export_dir")
    if export_dir is None:
      raise KeyError(
        "Missing export_dir in slide_entry")
    flat_files_dir = Path(export_dir) / flat_files_dir_name
    ## step 5: collect metadata json path
    metadata_json_file = slide_entry.get(metadata_json_key)
    if metadata_json_file is None:
      raise KeyError(
        f"No metadata json file found for slide {slide_id}")
    check_file_path(metadata_json_file)
    ## step 6: parse metadata json file and get panel info
    with open(metadata_json_file, 'r') as fp:
      json_data = json.load(fp)
    panel_name = json_data.get(panel_name_key)
    ## step 7: run notebook and generate report
    temp_dir = get_temp_dir(use_ephemeral_space=True)
    input_list = [
      COSMX_COUNT_FOV_REPORT_TEMPLATE,
      COSMX_QC_REPORT_IMAGE1,
      flat_files_dir,
      temp_dir]
    for f in input_list:
      check_file_path(f)
    container_bind_dir_list = [
      export_dir,
      temp_dir]
    date_tag = get_date_stamp()
    input_params = dict(
      DATE_TAG=date_tag,
      COSMX_PROJECT_NAME=project_igf_id,
      COSMX_SLIDE_NAME=slide_id,
      SLIDE_FLAT_FILE_DIR=flat_files_dir,
      SLIDE_METADATA_JSON_FILE=metadata_json_file,
      PANEL_NAME=panel_name)
    nb = \
      Notebook_runner(
        template_ipynb_path=COSMX_COUNT_FOV_REPORT_TEMPLATE,
        output_dir=temp_dir,
        input_param_map=input_params,
        container_paths=container_bind_dir_list,
        kernel='r',
        use_ephemeral_space=True,
        singularity_options=['-C'],
        allow_errors=False,
        singularity_image_path=COSMX_QC_REPORT_IMAGE1,
        timeout=1200,
        no_input=False)
    output_notebook, _ = \
      nb.execute_notebook_in_singularity()
    ## step 8: copy report to reports dir
    report_dir = Path(export_dir) / report_files_dir_name
    os.makedirs(report_dir, exist_ok=True)
    target_notebook_path = \
      report_dir / os.path.basename(output_notebook)
    copy_local_file(
      output_notebook, target_notebook_path.as_posix())
    ## step 9: return new slide entry
    new_slide_entry = {
      "cosmx_run_id": cosmx_run_id,
      "slide_id": slide_id,
      "export_dir": export_dir,
      metadata_json_key: metadata_json_file,
      "flatfiles_dir": flat_files_dir}
    return new_slide_entry
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))
    raise ValueError(e)

def fetch_slide_annotations_from_design_file(
  design_file: str,
  cosmx_slide_id: str,
  analysis_metadata_key: str = "analysis_metadata",
  annotation_key: str = "annotation",
  cosmx_slide_id_key: str = "cosmx_slide_id",
  tissue_annotation_key: str = "tissue_annotation",
  tissue_ontology_key: str = "tissue_ontology",
  tissue_condition_key: str = "tissue_condition") \
    -> Tuple[str, str, str]:
  try:
    with open(design_file, 'r') as fp:
      design_data = load(fp, Loader=SafeLoader)
    analysis_metadata = design_data.get(analysis_metadata_key)
    annotation = analysis_metadata.get(annotation_key)
    if annotation is None:
      return "UNKNOWN", "UNKNOWN", "UNKNOWN"
    annotation_entry = \
      [f for f in annotation \
        if f.get(cosmx_slide_id_key) == cosmx_slide_id]
    if len(annotation_entry) == 0:
      return "UNKNOWN", "UNKNOWN", "UNKNOWN"
    else:
      annotation_entry = annotation_entry[0]
    if not isinstance(annotation_entry, dict):
      raise TypeError(
        f"Expecting a dictionary, got {type(annotation_entry)}")
    tissue_annotation = \
      annotation_entry.get(tissue_annotation_key, "UNKNOWN")
    tissue_ontology = \
      annotation_entry.get(tissue_ontology_key, "UNKNOWN")
    tissue_condition = \
      annotation_entry.get(tissue_condition_key, "UNKNOWN")
    return tissue_annotation, tissue_ontology, tissue_condition
  except Exception as e:
    raise ValueError(
      f"Failed to get slide annotation, error: {e}")


def fetch_cosmx_metadata_info(
  cosmx_metadata_json: str,
  platform_name_key: str = "Instrument",
  fov_range_key: str = "FOV Range",
  slot_id_key: str = "Slot ID",
  run_tissue_name_key: str = "Run_Tissue_name",
  panel_info_key: str = "Panel",
  version_key: str = "version",
  assay_type_key: str = "assay_type") \
    -> Dict[str, Any]:
  """
  A function to fetch COSMX metadata from a json file

  :param cosmx_metadata_json: Path to the COSMX metadata json file
  :param platform_name_key: Key for the platform name in the json
  :param fov_range_key: Key for the FOV range in the json
  :param slot_id_key: CosMX slide run date key in the json
  :param run_tissue_name_key: CosMX slide run tissue name key
  :param panel_info_key: Key for the panel information in the json
  :param version_key: Key for the version in the json
  :param assay_type_key: Key for the assay type in the json
  :returns: A dictionary containing following keys:
    - fov_range: FOV range as a string
    - cosmx_platform_igf_id: COSMX platform IGF ID as a string
    - run_tissue_name: COSMX slide name
    - slide_run_date: COSMX slide run date as datetime
    - panel_info: Panel information as a string
    - assay_type: Assay type as a string
    - version: Version as a string
    - metadata_json_entry: The entire metadata json entry as a dictionary
  """
  try:
    with open(cosmx_metadata_json, "r") as fp:
      metadata_json_entry = \
        json.load(fp)
    fov_range = metadata_json_entry.get(fov_range_key)
    cosmx_platform_igf_id = metadata_json_entry.get(platform_name_key)
    panel_info = metadata_json_entry.get(panel_info_key)
    assay_type = metadata_json_entry.get(assay_type_key)
    version = metadata_json_entry.get(version_key)
    slot_id = metadata_json_entry.get(slot_id_key)
    run_tissue_name = metadata_json_entry.get(run_tissue_name_key)
    if fov_range is None or \
       cosmx_platform_igf_id is None or \
       panel_info is None or \
       assay_type is None or \
       slot_id is None or \
       run_tissue_name is None or \
       version is None:
      raise KeyError(
        f"Missing required cosmx metadata in the slide json file {cosmx_metadata_json}")
    ## convert slot id to slide run date using dateutil parser
    slide_run_date = \
      parse(slot_id.replace("_s3", "").replace("_", ""))
    output_dict = dict(
      fov_range=fov_range,
      cosmx_platform_igf_id=cosmx_platform_igf_id,
      run_tissue_name=run_tissue_name,
      slide_run_date=slide_run_date,
      panel_info=panel_info,
      assay_type=assay_type,
      version=version,
      metadata_json_entry=metadata_json_entry)
    return output_dict
  except Exception as e:
    raise ValueError(
      f"Failed to get metadata, error: {e}")


def load_cosmx_data_to_db(
  project_igf_id: str,
  db_config_file: str,
  cosmx_run_id: str,
  cosmx_slide_id: str,
  cosmx_slide_name: str,
  cosmx_platform_id: str,
  cosmx_slide_panel_info: str,
  cosmx_slide_assay_type: str,
  cosmx_slide_version: str,
  cosmx_slide_run_date: datetime,
  cosmx_slide_metadata: Dict[str, str],
  cosmx_count_json_file: str,
  cosmx_slide_fov_range: str,
  tissue_annotation: str,
  tissue_ontology: str,
  tissue_condition: str,
  rna_count_file_validation_schema: str,
  protein_count_file_validation_schema: str) -> None:
  try:
    ## step x: register cosmx run
    run_registration_status = \
      check_and_register_cosmx_run(
        project_igf_id=project_igf_id,
        cosmx_run_igf_id=cosmx_run_id,
        db_session_class=None)
    ## step x: register cosmx slide
    slide_registration_status = \
      check_and_register_cosmx_slide(
        cosmx_run_igf_id=cosmx_run_id,
        cosmx_slide_igf_id=cosmx_slide_id,
        cosmx_platform_igf_id=cosmx_platform_id,
        panel_info=cosmx_slide_panel_info,
        assay_type=cosmx_slide_assay_type,
        version=cosmx_slide_version,
        db_session_class=None,
        slide_metadata=cosmx_slide_metadata)
    ## step x: fov registration
    fov_registration_status = \
      create_or_update_cosmx_slide_fov(
        cosmx_slide_igf_id=cosmx_slide_id,
        fov_range=cosmx_slide_fov_range,
        slide_type=cosmx_slide_version,
        db_session_class=None)
    ## step x: fov count qc registration
    fov_count_registration_status = \
      create_cosmx_slide_fov_count_qc(
        cosmx_slide_igf_id=cosmx_slide_id,
        fov_range=cosmx_slide_fov_range,
        slide_type=cosmx_slide_version,
        db_session_class=None,
        slide_count_json_file=cosmx_count_json_file,
        rna_count_file_validation_schema=rna_count_file_validation_schema,
        protein_count_file_validation_schema=protein_count_file_validation_schema)
    ## step x: annotate slide fovs
  except Exception as e:
    raise ValueError(
      f"Failed to load data to db, error: {e}")


## TASK
@task(multiple_outputs=False)
def register_db_data(
  slide_entry: Dict[str, str],
  design_file: str,
  report_files_dir_name: str = 'Reports',
  metadata_json_key:str = "slide_metadata_json",
  count_qc_json_output_key: str = "json_output") -> Dict[str, str]:
  try:
    new_slide_entry = {}
    ## step 1: get analysis_id and project id
    analysis_id, project_igf_id = \
      get_analysis_id_and_project_igf_id_from_airflow_dagrun_conf(
        database_config_file=DATABASE_CONFIG_FILE)
    ## step 2: get slide id and run id
    slide_id = slide_entry.get("slide_id")
    if slide_id is None:
      raise KeyError(
        "Missing slide_id in slide_entry")
    cosmx_run_id = slide_entry.get("cosmx_run_id")
    if cosmx_run_id is None:
      raise KeyError(
        "Missing cosmx_run_id in slide_entry")
    ## step 3: get metadata json file
    metadata_json_file = \
      slide_entry.get(metadata_json_key)
    if metadata_json_file is None:
      raise KeyError(
        f"No metadata json file found for slide {slide_id}")
    check_file_path(metadata_json_file)
    ## get count qc json file
    count_qc_json_output_dir = \
      slide_entry.get(count_qc_json_output_key)
    if count_qc_json_output_dir is None:
      raise KeyError(
        f"No count QC json dir found for slide {slide_id}")
    check_file_path(count_qc_json_output_dir)
    ## parse metadata json file
    slide_metadata_info = \
      fetch_cosmx_metadata_info(
        cosmx_metadata_json=metadata_json_file)
    fov_range = slide_metadata_info.get("fov_range")
    cosmx_platform_igf_id = slide_metadata_info.get("cosmx_platform_igf_id")
    run_tissue_name = slide_metadata_info.get("run_tissue_name")
    slide_run_date = slide_metadata_info.get("slide_run_date")
    panel_info = slide_metadata_info.get("panel_info")
    assay_type = slide_metadata_info.get("assay_type")
    version = slide_metadata_info.get("version")
    metadata_json_entry = slide_metadata_info.get("metadata_json_entry")
    ## fetch analysis description and parse fov anotation from analysis description
    tissue_annotation, tissue_ontology, tissue_condition = \
      fetch_slide_annotations_from_design_file(
        design_file=design_file,
        cosmx_slide_id=slide_id)
    ## prep table data for db upload
    ## load data to table



    
    return new_slide_entry
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))
    raise ValueError(e)


## TASK
@task(multiple_outputs=False)
def copy_slide_data_to_globus(slide_entry: Dict[str, str]) -> Dict[str, str]:
  try:
    new_slide_entry = {}
    return new_slide_entry
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))
    raise ValueError(e)


## TASK
@task(multiple_outputs=False)
def generate_additional_qc_report1(
  slide_entry: Union[List[Dict[str, str]], Any]) \
    -> Optional[bool]:
  try:
    return None
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))


## TASK
@task(multiple_outputs=False)
def generate_additional_qc_report2(
  slide_entry: Union[List[Dict[str, str]], Any]) \
    -> Optional[bool]:
  try:
    return None
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))


## TASK
@task(multiple_outputs=False)
def upload_reports_to_portal(
    slide_entry: Union[List[Dict[str, str]], Any]) \
      -> Optional[bool]:
  try:
    return None
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))