import os
import shutil
import logging
import pandas as pd
from datetime import timedelta
from airflow.models import Variable
from igf_data.utils.bashutils import bash_script_wrapper
from igf_data.utils.analysis_fastq_fetch_utils import get_fastq_and_run_for_samples
from igf_data.utils.jupyter_nbconvert_wrapper import Notebook_runner
from typing import (
    Tuple,
    Optional)
from igf_data.utils.fileutils import (
  check_file_path,
  copy_local_file,
  get_temp_dir,
  get_date_stamp)
from igf_airflow.logging.upload_log_msg import send_log_to_channels
from igf_airflow.utils.dag22_bclconvert_demult_utils import (
  _create_output_from_jinja_template)
from igf_airflow.utils.generic_airflow_utils import (
    get_project_igf_id_for_analysis,
    fetch_analysis_name_for_analysis_id,
    send_airflow_failed_logs_to_channels,
    collect_analysis_dir,
    parse_analysis_design_and_get_metadata
)
from airflow.operators.python import get_current_context
from airflow.decorators import task

log = logging.getLogger(__name__)

## CONF
SLACK_CONF = \
  Variable.get('analysis_slack_conf', default_var=None)
MS_TEAMS_CONF = \
  Variable.get('analysis_ms_teams_conf', default_var=None)
DATABASE_CONFIG_FILE = \
  Variable.get('database_config_file', default_var=None)
SPACERANGER_COUNT_SCRIPT_TEMPLATE = \
  Variable.get("spaceranger_count_script_template", default_var=None)
SPACERANGER_AGGR_SCRIPT_TEMPLATE = \
  Variable.get("spaceranger_aggr_script_template", default_var=None)

def get_spaceranger_analysis_design_and_get_groups(design_file: str) -> list:
  try:
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
    unique_sample_groups = list()
    for sample_name, sample_data in sample_metadata.items():
      unique_sample_groups.\
        append({
          "sample_metadata": {
            sample_name: sample_data},
          "analysis_metadata": analysis_metadata})
    if len(unique_sample_groups) == 0:
      raise ValueError("No sample group found")
    return unique_sample_groups
  except Exception as e:
    raise ValueError(
      f"Failed to get groups for spaceranger analysis, error: {e}")


## TASK
@task(
  task_id="get_spaceranger_analysis_group_list",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G',
  multiple_outputs=False)
def get_spaceranger_analysis_group_list(design_dict: dict) -> list:
  try:
    design_file = design_dict.get('analysis_design')
    unique_sample_groups = \
      get_spaceranger_analysis_design_and_get_groups(
        design_file=design_file)
    return unique_sample_groups
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


def prepare_spaceranger_count_run_dir_and_script_file(
      sample_metadata: dict,
      analysis_metadata: dict,
      db_config_file: str,
      run_script_template: str,
      spaceranger_config_key: str = "spaceranger_count_config") \
        -> Tuple[str, str, str]:
  try:
    ## check if sample info is present
    sample_list = \
      list(sample_metadata.keys())
    if len(sample_list) == 0:
      raise ValueError(
        f"No sample id found in the sample metadata: {sample_metadata}")
    sample_id = sample_list[0]
    sample_info = sample_metadata.get(sample_id)
    if sample_info is None:
      raise ValueError(
        f"No sample info found for sample {sample_id}")
    ## check if input files are present
    check_file_path(db_config_file)
    check_file_path(run_script_template)
    ## get fastq files for all samples
    fastq_list = \
      get_fastq_and_run_for_samples(
        dbconfig_file=db_config_file,
        sample_igf_id_list=[sample_id,])
    if len(fastq_list) == 0:
      raise ValueError(
        "No fastq file found for samples")
    ## get fastq dirs
    df = pd.DataFrame(fastq_list)
    df['fastqs'] = \
      df['file_path'].\
        map(lambda x: os.path.dirname(x))
    fastqs = \
      df['fastqs'].\
        drop_duplicates().\
        values.\
        tolist()
    fastqs = \
      ','.join(fastqs)
    # set parameters
    spaceranger_params = list()
    ## image parameters
    for param_key, param_val in sample_info.items():
      if param_val is not None:
        spaceranger_params.\
          append(f"--{param_key}={param_val}")
    if len(spaceranger_params) == 0:
      raise ValueError(
        f"No image param found for sample {sample_id}")
    spaceranger_count_config = \
      analysis_metadata.get(spaceranger_config_key)
    if spaceranger_count_config is None or \
       not isinstance(spaceranger_count_config, list) or \
       len(spaceranger_count_config) == 0:
      raise ValueError(
        f"spaceranger_count_config is not correct: {spaceranger_count_config}")
    ## add analysis config
    spaceranger_params.extend(
      spaceranger_count_config)
    spaceranger_params = \
      ' \\\n'.join(spaceranger_params)
    ## create run script from template
    work_dir = \
      get_temp_dir(use_ephemeral_space=True)
    script_file = \
      os.path.join(
        work_dir,
        os.path.basename(run_script_template))
    output_dir = \
      os.path.join(work_dir, sample_id)
    _create_output_from_jinja_template(
      template_file=run_script_template,
      output_file=script_file,
      autoescape_list=['xml',],
      data=dict(
        SPACERANGER_ID=str(sample_id),
        FASTQS=fastqs,
        SPACERANGER_PARAMS=spaceranger_params,
        WORKDIR=work_dir))
    return sample_id, script_file, output_dir
  except Exception as e:
    raise ValueError(
      f"Failed to create spaceranger count script and dir, error: {e}")




## TASK
@task(
  task_id="prepare_spaceranger_count_script",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G',
  multiple_outputs=False)
def prepare_spaceranger_count_script(analysis_entry: dict) \
      -> dict:
  try:
    sample_metadata = \
      analysis_entry.get("sample_metadata")
    analysis_metadata = \
      analysis_entry.get("analysis_metadata")
    sample_id, run_script_file, output_dir = \
      prepare_spaceranger_count_run_dir_and_script_file(
        sample_metadata=sample_metadata,
        analysis_metadata=analysis_metadata,
        db_config_file=DATABASE_CONFIG_FILE,
        run_script_template=SPACERANGER_COUNT_SCRIPT_TEMPLATE)
    return {"sample_id": sample_id,
            "script_file": run_script_file,
            "output_dir": output_dir}
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


## TASK
@task(
  task_id="run_spaceranger_count_script",
  retry_delay=timedelta(minutes=15),
  retries=10,
  queue='hpc_8G4t72hr',
  pool='batch_job',
  multiple_outputs=False)
def run_spaceranger_count_script(analysis_script_info: dict) \
      -> dict:
  try:
    sample_id = analysis_script_info.get("sample_id")
    script_file = analysis_script_info.get("script_file")
    output_dir = analysis_script_info.get("output_dir")
    ## check for _lock file
    lock_file = \
      os.path.join(output_dir, '_lock')
    if os.path.exists(lock_file):
      raise ValueError(
        f"""Lock file exists in spaceranger run path: {output_dir}. \
            Remove it to continue!""")
    try:
      _, _ = \
        bash_script_wrapper(
          script_path=script_file,
          capture_stderr=False)
    except Exception as e:
      raise ValueError(
        f"Failed to run spaceranger script, Script: {script_file} for sample: {sample_id}")
    ## check output dir exists
    check_file_path(output_dir)
    return {"sample_id": sample_id,
            "output_dir": output_dir}
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


## TG1 TASK: run squidpy qc step
## TASK
@task(
  task_id="run_squidpy_qc",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G',
  multiple_outputs=False)
def run_squidpy_qc(analysis_output: dict) -> dict:
  try:
    sample_id = analysis_output.get("sample_id")
    output_dir = analysis_output.get("output_dir")
    ## generate report and move it to visium output directory
    return {"sample_id": sample_id, "output_dir": output_dir}
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


## TASK
@task(
  task_id="move_single_spaceranger_count_to_main_work_dir",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def move_single_spaceranger_count_to_main_work_dir(
      work_dir: str,
      analysis_output: dict) -> dict:
  try:
    check_file_path(work_dir)
    sample_id = analysis_output.get("sample_id")
    output_dir = analysis_output.get("output_dir")
    target_spaceranger_count_dir = \
      os.path.join(
        work_dir,
        os.path.basename(output_dir))
    ## not safe to overwrite existing dir
    if os.path.exists(target_spaceranger_count_dir):
      raise IOError(
        f"""spaceranger output path for sample {sample_id}) already present. \
          Path: {target_spaceranger_count_dir}. \
          CLEAN UP and RESTART !!!""")
    shutil.move(
      output_dir,
      work_dir)
    output_dict = {
      "sample_id": sample_id,
      "output": target_spaceranger_count_dir}
    return output_dict
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


## TASK: collect all analysis outputs
@task(
  task_id="collect_spaceranger_count_analysis",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def collect_spaceranger_count_analysis(
      analysis_output_list: list) -> list:
  try:
    return analysis_output_list
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)



## TASK: switch to aggr if morethan one samples are 
@task(
  task_id="decide_aggr",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def decide_aggr(
      analysis_output_list: list,
      aggr_task: str = "prepare_spaceranger_aggr_script",
      non_aggr_task: str = "calculate_md5_for_work_dir") -> list:
  try:
    if len(analysis_output_list) > 1:
      return [aggr_task]
    elif len(analysis_output_list) == 1:
      return [non_aggr_task]
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


## TASK: prep aggr run script
@task(
  task_id="prepare_spaceranger_aggr_script",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G',
  multiple_outputs=False)
def prepare_spaceranger_aggr_script(analysis_output_list: list) -> str:
  try:
    spaceranger_count_dict = dict()
    for entry in analysis_output_list:
      if entry is not None:
        sample_id = entry.get("sample_id")
        count_dir = entry.get("output")
        spaceranger_count_dict.update({
          sample_id: count_dir})
    script_file, output_dir = \
      prepare_spaceranger_aggr_run_dir_and_script(
        spaceranger_count_dict=spaceranger_count_dict,
        spaceranger_aggr_script_template=SPACERANGER_AGGR_SCRIPT_TEMPLATE)
    return {"script_file": script_file, "output_dir": output_dir}
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


def prepare_spaceranger_aggr_run_dir_and_script(
    spaceranger_count_dict: dict,
    spaceranger_aggr_script_template: str,
    spaceranger_aggr_params: Optional[list] = None) \
      -> Tuple[str, str]:
  try:
    required_fields = {
      "molecule_h5": "outs/molecule_info.h5",
      "cloupe_file": "outs/cloupe.cloupe",
      "spatial_folder": "outs/spatial"}
    aggr_csv_list = list()
    for sample_id, count_dir_path in spaceranger_count_dict.items():
      row_data = {"library_id": sample_id}
      for required_field_name, required_field_val in required_fields.items():
        required_field_path = \
          os.path.join(
            count_dir_path,
            required_field_val)
        row_data.update({
          required_field_name: required_field_path})
      aggr_csv_list.append(row_data)
    ## check if any info is present or not
    if len(aggr_csv_list) == 0:
      raise ValueError(
        f"No count dir info found in {spaceranger_count_dict}")
    ## get work dir and create scripts
    work_dir = \
      get_temp_dir(
        use_ephemeral_space=True)
    ## aggr csv
    aggr_csv_filepath = \
      os.path.join(
        work_dir,
        "spaceranger_aggr_input.csv")
    pd.DataFrame(aggr_csv_list).\
      to_csv(
        aggr_csv_filepath,
        index=False)
    ## script
    aggr_script_path = \
      os.path.join(
        work_dir,
        os.path.basename(
          spaceranger_aggr_script_template))
    output_dir = \
      os.path.join(work_dir, "ALL")
    if spaceranger_aggr_params is None:
      spaceranger_aggr_params = list()
    spaceranger_aggr_params = \
      ' '.join(spaceranger_aggr_params)
    _create_output_from_jinja_template(
      template_file=spaceranger_aggr_script_template,
      output_file=aggr_script_path,
      autoescape_list=['xml',],
      data=dict(
        SPACERANGER_ID="ALL",
        CAV_FILE=aggr_csv_filepath,
        SPACERANGER_AGGR_PARAMS=spaceranger_aggr_params,
        WORKDIR=work_dir))
    return aggr_script_path, output_dir
  except Exception as e:
    raise ValueError(
      f"Failed to create spaceranger aggr run script, error: {e}")


## TASK
@task(
  task_id="run_spaceranger_aggr_script",
  retry_delay=timedelta(minutes=15),
  retries=10,
  queue='hpc_8G4t72hr',
  pool='batch_job',
  multiple_outputs=False)
def run_spaceranger_aggr_script(aggr_script_info: dict) \
      -> str:
  try:
    script_file = aggr_script_info.get("script_file")
    output_dir = aggr_script_info.get("output_dir")
    ## check for _lock file
    lock_file = \
      os.path.join(output_dir, '_lock')
    if os.path.exists(lock_file):
      raise ValueError(
        f"""Lock file exists in spaceranger run path: {output_dir}. \
            Remove it to continue!""")
    try:
      _, _ = \
        bash_script_wrapper(
          script_path=script_file,
          capture_stderr=False)
    except Exception as e:
      raise ValueError(
        f"Failed to run spaceranger aggr script, Script: {script_file}")
    ## check output dir exists
    check_file_path(output_dir)
    return output_dir
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


## TASK
@task(
  task_id="squidpy_qc_for_aggr",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G',
  multiple_outputs=False)
def squidpy_qc_for_aggr(analysis_output_dir: str) -> str:
  try:
    return analysis_output_dir
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


## TASK
@task(
  task_id="move_spaceranger_aggr_to_main_work_dir",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def move_spaceranger_aggr_to_main_work_dir(
      work_dir: str,
      analysis_output_dir: str) -> str:
  try:
    check_file_path(work_dir)
    target_spaceranger_count_dir = \
      os.path.join(
        work_dir,
        os.path.basename(analysis_output_dir))
    ## not safe to overwrite existing dir
    if os.path.exists(target_spaceranger_count_dir):
      raise IOError(
        f"""spaceranger aggr output path already present. \
          Path: {target_spaceranger_count_dir}. \
          CLEAN UP and RESTART !!!""")
    shutil.move(
      analysis_output_dir,
      work_dir)
    output_dict = {
      "sample_id": "ALL",
      "output": target_spaceranger_count_dir}
    return output_dict
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)