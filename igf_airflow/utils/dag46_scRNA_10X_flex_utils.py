import os
import logging
import pandas as pd
from typing import Any
from datetime import timedelta
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
from igf_airflow.utils.generic_airflow_utils import (
  send_airflow_failed_logs_to_channels)
from igf_data.utils.fileutils import (
  check_file_path,
  copy_local_file,
  get_temp_dir,
  get_date_stamp)
from igf_airflow.utils.dag34_cellranger_multi_scRNA_utils import (
  prepare_cellranger_run_dir_and_script_file,
  parse_analysis_design_and_get_metadata,
  get_project_igf_id_for_analysis,
  fetch_analysis_name_for_analysis_id,
  prepare_and_run_scanpy_notebook)

log = logging.getLogger(__name__)

MS_TEAMS_CONF = \
  Variable.get('analysis_ms_teams_conf', default_var=None)
DATABASE_CONFIG_FILE = \
  Variable.get('database_config_file', default_var=None)
HPC_BASE_RAW_DATA_PATH = \
  Variable.get('hpc_base_raw_data_path', default_var=None)
HPC_FILE_LOCATION = \
  Variable.get("hpc_file_location", default_var="HPC_PROJECT")

## CELLRANGER
CELLRANGER_MULTI_SCRIPT_TEMPLATE = \
  Variable.get("cellranger_multi_script_template", default_var=None)

def _get_cellranger_sample_group(
  sample_metadata: dict[str, Any],
  cellranger_group_name: str = 'cellranger_group',
  required_tag_name: str = 'feature_types',
  required_tag_value: str = 'Gene Expression') \
    -> dict[str]:
  try:
    unique_sample_groups = set()
    required_tag_list = list()
    for _, group in sample_metadata.items():
      grp_name = group.get(cellranger_group_name)
      if grp_name is None:
        raise KeyError(
          "Missing cellranger_group in sample_metadata")
      unique_sample_groups.add(grp_name)
      required_tag_list.append({
        "name": grp_name,
        required_tag_name: group.get(required_tag_name)})
    if len(unique_sample_groups) == 0:
      raise ValueError("No sample group found")
    unique_sample_groups = \
      list(unique_sample_groups)
    required_tag_df = \
      pd.DataFrame(required_tag_list)
    ## check for required tags
    for g in unique_sample_groups:
      g_tag_values = (
        required_tag_df[required_tag_df['name']==g][required_tag_name]
        .values
        .tolist()
      )
      if required_tag_value not in g_tag_values:
        raise KeyError(
          f"No {required_tag_value} found for group {g}")
    return unique_sample_groups
  except Exception as e:
    raise ValueError(
      f'Failed to get cellranger sample group, error: {e}')


## TASK
@task(
  task_id="prepare_cellranger_flex_script",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def prepare_cellranger_flex_script(
  design_dict: dict,
  work_dir: str,
  analysis_design_tag: str = "analysis_design",
  cellranger_group_name: str = 'cellranger_group',
  required_tag_name: str = 'feature_types',
  required_tag_value: str = 'Gene Expression') -> dict:
  
  """
  Create cellranger flex script
  """
  try:
    ## get sample metadata
    design_file = \
      design_dict.get(analysis_design_tag)
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
    ## check if only one sample group is present or not
    sample_groups = \
      _get_cellranger_sample_group(
        sample_metadata=sample_metadata,
        required_tag_name=required_tag_name,
        required_tag_value=required_tag_value)
    ## check if correct number of sample groups are present
    ## reset sample group if more than one groups are present
    if len(sample_groups) == 0:
      raise ValueError(
        "No sample group has been found, file: " + \
        f"{design_file}")
    sample_group = \
        sample_groups[0]
    library_csv_file, run_script_file = \
      prepare_cellranger_run_dir_and_script_file(
        sample_group=str(sample_group),
        work_dir=work_dir,
        output_dir=os.path.join(work_dir, str(sample_group)),
        design_file=design_file,
        db_config_file=DATABASE_CONFIG_FILE,
        run_script_template=CELLRANGER_MULTI_SCRIPT_TEMPLATE,
        cellranger_group_tag=cellranger_group_name,
        feature_types_tag=required_tag_name)
    analysis_script_info = {
      "sample_group": sample_group,
      "run_script": run_script_file,
      "output_dir": os.path.join(work_dir, sample_group)}
    return analysis_script_info
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)

## TASK
@task(
  task_id="run_single_sample_scanpy",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_8G')
def run_single_sample_scanpy(
      sample_group: str,
      cellranger_output_dir: str,
      design_dict: dict) -> dict:
  try:
    ## set cellranger counts dir
    cellranger_counts_dir = \
      os.path.join(
        cellranger_output_dir,
        'outs',
        'per_sample_outs',
        sample_group,
        'count')
    ## set scanpy dir
    scanpy_output_dir = \
      os.path.join(
        cellranger_output_dir,
        'outs',
        'per_sample_outs',
        sample_group,
        'scanpy')
    os.makedirs(scanpy_output_dir, exist_ok=True)
    check_file_path(cellranger_counts_dir)
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
    scanpy_config = \
      analysis_metadata.get("scanpy_config")
    if scanpy_config is None or \
        not isinstance(scanpy_config, dict):
      raise KeyError(
        f"Missing scanpy_config in the design file: {design_file}")
    ## get project id
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
    output_notebook_path, scanpy_h5ad = \
      prepare_and_run_scanpy_notebook(
        project_igf_id=project_igf_id,
        analysis_name=analysis_name,
        cellranger_group_id=str(sample_group),
        cellranger_counts_dir=cellranger_counts_dir,
        scanpy_config=scanpy_config)
    ## copy output files to scanpy output dir
    target_notebook_path = \
      os.path.join(
        scanpy_output_dir,
        os.path.basename(output_notebook_path))
    copy_local_file(
      output_notebook_path,
      target_notebook_path,
      force=True)
    target_scanpy_h5ad = \
      os.path.join(
        scanpy_output_dir,
        os.path.basename(scanpy_h5ad))
    copy_local_file(
      scanpy_h5ad,
      target_scanpy_h5ad,
      force=True)
    check_file_path(target_notebook_path)
    check_file_path(target_scanpy_h5ad)
    output_dict = {
      "sample_group": sample_group,
      "cellranger_output_dir": cellranger_output_dir,
      "notebook_report": target_notebook_path,
      "scanpy_h5ad": target_scanpy_h5ad}
    return output_dict
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)