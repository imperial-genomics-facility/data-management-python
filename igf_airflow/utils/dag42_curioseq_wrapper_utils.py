import os
import re
import shutil
import logging
import subprocess
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
from igf_airflow.utils.dag22_bclconvert_demult_utils import (
  _create_output_from_jinja_template)
from igf_airflow.utils.generic_airflow_utils import (
    get_project_igf_id_for_analysis,
    fetch_analysis_name_for_analysis_id,
    send_airflow_failed_logs_to_channels,
    send_airflow_pipeline_logs_to_channels,
    get_per_sample_analysis_groups,
    collect_analysis_dir,
    parse_analysis_design_and_get_metadata
)
from airflow.operators.python import get_current_context
from airflow.decorators import task

log = logging.getLogger(__name__)

## CONF
SLACK_CONF = \
  Variable.get(
      'analysis_slack_conf',
      default_var=None)
MS_TEAMS_CONF = \
  Variable.get(
      'analysis_ms_teams_conf', default_var=None)
DATABASE_CONFIG_FILE = \
  Variable.get('database_config_file', default_var=None)
CURIOSEEKER_TEMPLATE = \
  Variable.get('curioseeker_template', default_var=None)
CURIOSEEKER_NF_CONFIG_TEMPLATE = \
  Variable.get('curioseeker_nf_config_template', default_var=None)

## TASK
@task(
  task_id="get_curioseeker_analysis_group_list",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G',
  multiple_outputs=False)
def get_curioseeker_analysis_group_list(design_dict: dict) -> list:
  try:
    design_file = design_dict.get('analysis_design')
    unique_sample_groups = \
      get_per_sample_analysis_groups(
        design_file=design_file)
    return unique_sample_groups
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


## TASK
@task(
  task_id="merge_fastqs_for_analysis",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G',
  multiple_outputs=False)
def merge_fastqs_for_analysis(analysis_entry: dict) -> dict:
  try:
    sample_metadata = \
      analysis_entry.get("sample_metadata")
    ## not filtering multiple samples at this stage
    output_sample_metadata = \
      fetch_and_merge_fastqs_for_samples(
        sample_dict=sample_metadata,
        db_config_file=DATABASE_CONFIG_FILE)
    return  output_sample_metadata
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


def fetch_and_merge_fastqs_for_samples(
      sample_dict: dict,
      db_config_file: str,
      fastq_file_input_column: str = 'file_path') \
        -> dict:
  try:
    fastq1_pattern = \
      re.compile(r'\S+_R1_001.fastq.gz')
    output_dict = dict()
    for sample_id, sample_info in sample_dict.items():
      ## get fastq files for all samples
      fastq_list = \
        get_fastq_and_run_for_samples(
          dbconfig_file=db_config_file,
          sample_igf_id_list=[sample_id,])
      if len(fastq_list) == 0:
        raise ValueError(
          "No fastq file found for samples")
      ## get fastq dirs
      fastq_df = pd.DataFrame(fastq_list)
      fastq_files = \
        fastq_df[fastq_file_input_column].\
          values.tolist()
      ## find R1 files
      r1_fastq_list = [
        f for f in fastq_files
          if re.search(fastq1_pattern, f)]
      ## create R2 list
      r2_fastq_list = [
        f.replace("_R1_", "_R2_")
          for f in r1_fastq_list]
      ## check if R1 file is present for sample
      if len(r1_fastq_list) == 0:
        raise ValueError(
          f"Missing R1 fastq for sample {sample_id}")
      ## check if merged is needed
      merged_r1_fastq, merged_r2_fastq = \
        merge_list_of_r1_and_r2_files(
          r1_list=r1_fastq_list,
          r2_list=r2_fastq_list)
      sample_info.update({
        "R1": merged_r1_fastq,
        "R2": merged_r2_fastq})
      output_dict.update({
        sample_id: sample_info})
    return output_dict
  except Exception as e:
    raise ValueError(
      f"Failed to merge fastqs, error: {e}")


def merge_list_of_r1_and_r2_files(
      r1_list: list,
      r2_list: list,
      merged_r1_filename: str = "merged_R1_001.fastq.gz",
      merged_r2_filename: str = "merged_R2_001.fastq.gz")\
        -> Tuple[str, str]:
  try:
    merged_r1_fastq = None
    merged_r2_fastq = None
    if len(r1_list)==0 or \
       len(r2_list)==0:
      raise ValueError(
        f"Missing R1 or R2")
    if len(r1_list) != len(r2_list):
      raise ValueError(
        f"R1 or R2 list are not matching")
    if len(r1_list)==1:
      merged_r1_fastq = r1_list[0]
      merged_r2_fastq = r2_list[0]
    else:
      ## merge list of r1 and r2 files
      work_dir = get_temp_dir()
      merged_r1_fastq = \
        os.path.join(
          work_dir,
          merged_r1_filename)
      merged_r2_fastq = \
        os.path.join(
          work_dir,
          merged_r2_filename)
      cmd_r1 = \
        f"cat {' '.join(r1_list)} > {merged_r1_fastq}"
      cmd_r2 = \
        f"cat {' '.join(r2_list)} > {merged_r2_fastq}"
      subprocess.\
        check_call(cmd_r1, shell=True)
      subprocess.\
        check_call(cmd_r2, shell=True)
    ## check if merged files are found
    if merged_r1_fastq is None or \
       merged_r2_fastq is None:
      raise ValueError(
        f"No merged file found")
    check_file_path(merged_r1_fastq)
    check_file_path(merged_r2_fastq)
    return merged_r1_fastq, merged_r2_fastq
  except Exception as e:
    raise ValueError(
      f"Failed to merge fastqs, error: {e}")


## TASK
@task(
  task_id="prepare_curioseeker_analysis_scripts",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G',
  multiple_outputs=False)
def prepare_curioseeker_analysis_scripts(
      analysis_entry: dict,
      modified_sample_metadata: dict) -> dict:
  try:
    analysis_metadata = \
      analysis_entry.get("analysis_metadata")
    sample_id, _, _, run_script_file, output_dir = \
      prepare_curioseeker_run_dir_and_script_file(
        sample_metadata=modified_sample_metadata,
        analysis_metadata=analysis_metadata,
        run_script_template=CURIOSEEKER_TEMPLATE,
        nextflow_config_template=CURIOSEEKER_NF_CONFIG_TEMPLATE)
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


def prepare_curioseeker_run_dir_and_script_file(
      sample_metadata: dict,
      analysis_metadata: dict,
      run_script_template: str,
      nextflow_config_template: str,
      r1_fastq_tag: str = "R1",
      r2_fastq_tag: str = "R2",
      barcode_file_tag: str = "barcode_file",
      experiment_date_tag: str = "experiment_date",
      curioseeker_config_tag: str = "curioseeker_config",
      genome_tag: str = "genome",
      sample_col: str = "sample",
      experiment_date_col: str = "experiment_date",
      barcode_file_col: str = "barcode_file",
      fastq_1_col: str = "fastq_1",
      fastq_2_col: str = "fastq_2",
      genome_col: str = "genome",
      samplesheet_filename: str = "samplesheet.csv",
      nextflow_params: Optional[list] = None,
      ) -> Tuple[str, str, str,str, str]:
  try:
    ## get work dir
    work_dir = get_temp_dir()
    singularity_mount_dir_list = [work_dir,]
    ## samplesheet
    samplesheet_header = [
      sample_col,
      experiment_date_col,
      barcode_file_col,
      fastq_1_col,
      fastq_2_col,
      genome_col]
    curioseeker_config = \
      analysis_metadata.get(
        curioseeker_config_tag)
    if curioseeker_config is None:
      raise KeyError(
        f"""Missing {curioseeker_config_tag} in
        analysis metadata: {analysis_metadata}""")
    genome = \
      curioseeker_config.get(genome_tag)
    if genome is None:
      raise KeyError(
        f"Missing {genome_tag} in analysis metadata: {curioseeker_config}")
    ## collected data for samplesheet csv file
    csv_data = list()
    sample_id_list = list()
    for sample_id, sample_info in sample_metadata.items():
      sample_id_list.append(sample_id)
      fastq_1 = sample_info.get(r1_fastq_tag)
      fastq_2 = sample_info.get(r2_fastq_tag)
      barcode_file = sample_info.get(barcode_file_tag)
      experiment_date = sample_info.get(experiment_date_tag)
      if fastq_1 is None or \
         fastq_2 is None or \
         barcode_file is None or \
         experiment_date is None:
        raise KeyError(
          f"""Missing required field in sample metadata for {sample_id}.
          Required {samplesheet_header}
          Metadata: {sample_info}""")
      csv_data.append({
        sample_col: sample_id,
        experiment_date_col: experiment_date,
        barcode_file_col: barcode_file,
        fastq_1_col: fastq_1,
        fastq_2_col: fastq_2,
        genome_col: genome})
      singularity_mount_dir_list.append(
        os.path.dirname(fastq_1))
      singularity_mount_dir_list.append(
        os.path.dirname(barcode_file))
    csv_file_path = \
      os.path.join(
        work_dir,
        samplesheet_filename)
    pd.DataFrame(csv_data).\
      to_csv(
        csv_file_path,
        index=False,
        columns=samplesheet_header)
    # create nf config template
    nextflow_config_path = \
      os.path.join(
        work_dir,
        os.path.basename(nextflow_config_template))
    singularity_mount_dir_list = \
      list(set(singularity_mount_dir_list))
    singularity_mount_dir_list = \
      ",".join(singularity_mount_dir_list)
    _create_output_from_jinja_template(
      template_file=nextflow_config_template,
      output_file=nextflow_config_path,
      autoescape_list=['xml',],
      data=dict(
        DIR_LIST=singularity_mount_dir_list))
    # create nf script
    ## additional nf params for future
    if nextflow_params is None:
      nextflow_params = list()
    nextflow_params = \
      " ".join(nextflow_params)
    sample_ids = "_".join(sample_id_list)
    output_dir = \
      os.path.join(
        work_dir,
        sample_ids)
    run_script_path = \
      os.path.join(
        work_dir,
        os.path.basename(run_script_template))
    _create_output_from_jinja_template(
      template_file=run_script_template,
      output_file=run_script_path,
      autoescape_list=['xml',],
      data=dict(
        WORKDIR=work_dir,
        SAMPLESHEET_CSV=csv_file_path,
        OUTPUT_DIR=output_dir,
        CONFIG_FILE=nextflow_config_path,
        NEXTFLOW_PARAMS=nextflow_params))
    return sample_ids, csv_file_path, nextflow_config_path, run_script_path, output_dir
  except Exception as e:
    raise ValueError(
      f"Failed to create script for curioseeker, error: {e}")


## TASK
@task(
  task_id="run_curioseeker_nf_script",
  retry_delay=timedelta(minutes=15),
  retries=10,
  queue='hpc_8G4t72hr',
  pool='batch_job',
  multiple_outputs=False)
def run_curioseeker_nf_script(analysis_script_info: dict) \
      -> dict:
  try:
    sample_id = analysis_script_info.get("sample_id")
    script_file = analysis_script_info.get("script_file")
    output_dir = analysis_script_info.get("output_dir")
    send_airflow_pipeline_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=\
        f"Started Curioseeker pipeline for sample: {sample_id}, NF script: {script_file}")
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
    send_airflow_pipeline_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=\
        f"Finished Curioseeker pipeline for sample: {sample_id}, NF script: {script_file}")
    return {"sample_id": sample_id,
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
  task_id="run_scanpy_qc",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G',
  multiple_outputs=False)
def run_scanpy_qc(analysis_output: dict) -> dict:
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
  task_id="run_scanpy_qc_for_all_samples",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G',
  multiple_outputs=False)
def run_scanpy_qc_for_all_samples(analysis_output_list: list) -> str:
  try:
    return "to do"
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)