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
    Tuple)
from igf_data.utils.fileutils import (
  check_file_path,
  copy_local_file,
  get_temp_dir,
  get_date_stamp)
from igf_airflow.logging.upload_log_msg import send_log_to_channels
from igf_airflow.utils.dag22_bclconvert_demult_utils import (
  _create_output_from_jinja_template)
from igf_airflow.utils.dag26_snakemake_rnaseq_utils import (
  parse_analysis_design_and_get_metadata,
  get_project_igf_id_for_analysis)
from igf_airflow.utils.dag33_geomx_processing_util import (
  fetch_analysis_name_for_analysis_id,
  calculate_md5sum_for_analysis_dir,
  collect_analysis_dir)
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
CELLRANGER_ARC_SCRIPT_TEMPLATE = \
  Variable.get("cellranger_arc_script_template", default_var=None)
CELLRANGER_ARC_AGGR_SCRIPT_TEMPLATE = \
  Variable.get("cellranger_arc_aggr_script_template", default_var=None)

## TASK
@task(
  task_id="prepare_cellranger_script",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def prepare_cellranger_arc_script(sample_group: str, design_dict: dict) -> dict:
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
      prepare_cellranger_arc_run_dir_and_script_file(
        sample_group=str(sample_group),
        work_dir=work_dir,
        design_file=design_file,
        db_config_file=DATABASE_CONFIG_FILE,
        run_script_template=CELLRANGER_ARC_SCRIPT_TEMPLATE)
    output_dict = {
      "sample_group": sample_group,
      "run_script": run_script_file,
      "output_dir": os.path.join(work_dir, sample_group)}
    return output_dict
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


def prepare_cellranger_arc_run_dir_and_script_file(
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
      create_library_information_for_multiome_sample_group(
        sample_group=sample_group,
        sample_metadata=sample_metadata,
        db_config_file=db_config_file)
    ## create temp dir and dump script and library.csv
    library_csv_file = \
      os.path.join(
        work_dir,
        library_csv_filename)
    sample_library_csv = \
      pd.DataFrame(sample_library_list).\
      to_csv(library_csv_file, index=False)
    ## get cellranger arc conf
    cellranger_arc_config = \
      analysis_metadata.get("cellranger_arc_config")
    if cellranger_arc_config is None:
      raise KeyError("Missing cellranger_arc_config in analysis design")
    cellranger_arc_config_ref = \
      cellranger_arc_config.get("reference")
    if cellranger_arc_config_ref is None:
      raise KeyError("Missing cellranger_arc_config reference in analysis design")
    cellranger_arc_config_params = \
      cellranger_arc_config.get("parameters")
    if cellranger_arc_config_params is None:
       cellranger_arc_config_params = []
    if cellranger_arc_config_params is not None and \
       not isinstance(cellranger_arc_config_params, list):
        raise TypeError(
          f"cellranger_arc_config_params are not list: {type(cellranger_arc_config_params)}")
    ## format cellranger_arc_config_params
    cellranger_arc_config_params = \
      " ".join(cellranger_arc_config_params)
    ## check ref path
    check_file_path(cellranger_arc_config_ref)
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
        CELLRANGER_ARC_ID=str(sample_group),
        CELLRANGER_ARC_CSV=library_csv_file,
        CELLRANGER_ARC_REFERENCE=cellranger_arc_config_ref,
        CELLRANGER_ARC_CONFIG_PARAMS=cellranger_arc_config_params,
        WORKDIR=work_dir))
    return library_csv_file, script_file
  except Exception as e:
    raise ValueError(
      f"Failed to prepare cellranger script, error: {e}")


def create_library_information_for_multiome_sample_group(
      sample_group: str,
      sample_metadata: dict,
      db_config_file: str) -> list:
  try:
    ## get cellranger group
    sample_group_dict = dict()
    sample_igf_id_list = list()
    for sample_igf_id, group in sample_metadata.items():
      grp_name = group.get('cellranger_group')
      library_type = group.get('library_type')
      if grp_name is None or library_type is None:
        raise KeyError(
          "Missing cellranger_group or library_type in sample_metadata ")
      if str(grp_name) == str(sample_group):
        sample_igf_id_list.append(sample_igf_id)
      sample_group_dict.update({ sample_igf_id: library_type})
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
      library_type = sample_group_dict.get(sample_igf_id)
      if library_type is None:
        raise KeyError(
          f"No library_type found for sample {sample_igf_id}")
      fastq_id = \
        os.path.basename(fastq_file_path).split("_")[0]
      sample_library_list.append({
        "fastqs": fastq_dir,
        "sample": fastq_id,
        "library_type": library_type})
    return sample_library_list
  except Exception as e:
    raise ValueError(
      f"Failed to prepare cellranger script, error: {e}")


## TASK
@task(
  task_id="configure_cellranger_arc_aggr_run",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def configure_cellranger_arc_aggr_run(
      design_dict: dict,
      xcom_pull_task_ids: str = 'collect_and_branch',
      xcom_pull_task_key: str = 'cellranger_output_dict') \
        -> dict:
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
    ## get cellranger arc aggr conf
    cellranger_arc_aggr_config = \
      analysis_metadata.get("cellranger_arc_aggr_config")
    if cellranger_arc_aggr_config is None:
      raise KeyError("Missing cellranger_arc_aggr_config in analysis design")
    cellranger_arc_aggr_config_ref = \
      cellranger_arc_aggr_config.get("reference")
    if cellranger_arc_aggr_config_ref is None:
      raise KeyError("Missing cellranger_arc_aggr_config reference in analysis design")
    cellranger_arc_aggr_config_params = \
      cellranger_arc_aggr_config.get("parameters")
    if cellranger_arc_aggr_config_params is None:
       cellranger_arc_aggr_config_params = []
    if cellranger_arc_aggr_config_params is not None and \
       not isinstance(cellranger_arc_aggr_config_params, list):
        raise TypeError(
          f"cellranger_arc_aggr_config_params are not list: {type(cellranger_arc_aggr_config_params)}")
    ## configure arc aggr
    cellranger_output_dict = dict()
    context = get_current_context()
    ti = context.get('ti')
    cellranger_output_dict = \
      ti.xcom_pull(
        task_ids=xcom_pull_task_ids,
        key=xcom_pull_task_key)
    if cellranger_output_dict is None or \
       (isinstance(cellranger_output_dict, dict) and \
       len(cellranger_output_dict)) == 0:
      raise ValueError(f"No cellranger output found")
    elif len(cellranger_output_dict) == 1:
      raise ValueError(f"Single cellranger output found. Can't merge it!")
    else:
      output_dict = \
        configure_cellranger_arc_aggr(
          run_script_template=CELLRANGER_ARC_AGGR_SCRIPT_TEMPLATE,
          cellranger_arc_aggr_config_ref=cellranger_arc_aggr_config_ref,
          cellranger_arc_aggr_config_params=cellranger_arc_aggr_config_params,
          cellranger_output_dict=cellranger_output_dict)
      return output_dict
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


def configure_cellranger_arc_aggr(
      run_script_template: str,
      cellranger_arc_aggr_config_ref: str,
      cellranger_arc_aggr_config_params: list,
      cellranger_output_dict: dict,
      atac_fragments_name: str = 'atac_fragments.tsv.gz'
      ) -> dict:
  try:
    work_dir = get_temp_dir(use_ephemeral_space=True)
    cellranger_aggr_input_list = list()
    for sample_id, cellranger_output_path in cellranger_output_dict.items():
      for root,_, files in os.walk(cellranger_output_path):
        for f in files:
          if f == atac_fragments_name:
            cellranger_aggr_input_list.\
              append({
                "library_id": sample_id,
                "atac_fragments": os.path.join(root, f),
                "per_barcode_metrics": os.path.join(root, "per_barcode_metrics.csv"),
                "gex_molecule_info": os.path.join(root, "gex_molecule_info.h5"),})
    output_csv_file = \
      os.path.join(work_dir, 'aggr_input.csv')
    df = pd.DataFrame(cellranger_aggr_input_list)
    df[["library_id", "atac_fragments", "per_barcode_metrics", "gex_molecule_info"]].\
      to_csv(output_csv_file, index=False)
    run_script_file = \
      os.path.join(
        work_dir,
        os.path.basename(run_script_template))
    ## join params
    cellranger_arc_aggr_config_params = \
      " ".join(cellranger_arc_aggr_config_params)
    ## check ref path
    check_file_path(cellranger_arc_aggr_config_ref)
    ## create run script from template
    _create_output_from_jinja_template(
      template_file=run_script_template,
      output_file=run_script_file,
      autoescape_list=['xml',],
      data=dict(
        CELLRANGER_ARC_AGGR_ID="ALL",
        CELLRANGER_ARC_AGGR_CSV=output_csv_file,
        CELLRANGER_ARC_AGGR_REFERENCE=cellranger_arc_aggr_config_ref,
        CELLRANGER_ARC_AGGR_PARAMS=cellranger_arc_aggr_config_params,
        WORKDIR=work_dir))
    output_dict = {
      "sample_name": "ALL",
      "run_script": run_script_file,
      "library_csv": output_csv_file,
      "run_dir": work_dir,
      "output_dir": os.path.join(work_dir, "ALL")}
    return output_dict
  except Exception as e:
    raise ValueError(
      f"Failed to configure cellranger aggr run, error: {e}")
