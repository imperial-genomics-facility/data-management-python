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
  parse_analysis_design_and_get_metadata)

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
  cellranger_group_name: str = 'cellranger_group',
  required_tag_name: str = 'feature_types',
  required_tag_value: str = 'Gene Expression') -> dict:
  
  """
  Create cellranger flex script
  """
  try:
    ## get sample metadata
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
    ## check if only one sample group is present or not
    sample_groups = \
      _get_cellranger_sample_group(
        sample_metadata=sample_metadata)
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)

