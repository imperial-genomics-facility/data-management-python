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
CELLRANGER_MULTI_SCRIPT_TEMPLATE = \
  Variable.get("cellranger_multi_script_template", default_var=None)
CELLRANGER_AGGR_SCRIPT_TEMPLATE = \
  Variable.get("cellranger_aggr_script_template", default_var=None)


## TASK
@task(
  task_id="get_analysis_group_list",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G',
  multiple_outputs=False)
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
  task_id="create_main_work_dir",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G',
  multiple_outputs=True)
def create_main_work_dir() -> dict:
  try:
    main_work_dir = get_temp_dir(use_ephemeral_space=True)
    return {"main_work_dir": main_work_dir}
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
        output_dir=os.path.join(work_dir, str(sample_group))
        design_file=design_file,
        db_config_file=DATABASE_CONFIG_FILE,
        run_script_template=CELLRANGER_MULTI_SCRIPT_TEMPLATE)
    return {"sample_group": sample_group, "run_script": run_script_file, "output_dir": os.path.join(work_dir, sample_group)}
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
      output_dir: str,
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
    # output_dir = \
    #    os.path.join(work_dir, str(sample_group))
    _create_output_from_jinja_template(
      template_file=run_script_template,
      output_file=script_file,
      autoescape_list=['xml',],
      data=dict(
        CELLRANGER_MULTI_ID=str(sample_group),
        CELLRANGER_MULTI_CSV=library_csv_file,
        CELLRANGER_MULTI_OUTPUT_DIR=output_dir,
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


## TASK
@task(
  task_id="run_cellranger_script",
  retry_delay=timedelta(minutes=15),
  retries=10,
  queue='hpc_8G4t72hr',
  pool='batch_job')
def run_cellranger_script(
      script_dict: dict) -> str:
  try:
    sample_group = script_dict.get('sample_group')
    run_script = script_dict.get('run_script')
    output_dir = script_dict.get('output_dir')
    try:
      stdout_file, stderr_file = \
        bash_script_wrapper(
          script_path=run_script)
    except Exception as e:
      raise ValueError(
        f"Failed to run script, Script: {run_script} for group: {sample_group}, error file: {stderr_file}")
    ## check output dir exists
    check_file_path(output_dir)
    return output_dir
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
  task_id="run_single_sample_scanpy",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def run_single_sample_scanpy(
      sample_group: str,
      cellranger_output_dir: str,
      design_dict: dict) -> dict:
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
        cellranger_output_dir=cellranger_output_dir,
        scanpy_config=scanpy_config)
    output_dict = {
      "sample_group": sample_group,
      "cellranger_output_dir": cellranger_output_dir,
      "notebook_report": output_notebook_path,
      "scanpy_h5ad": scanpy_h5ad}
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


def prepare_and_run_scanpy_notebook(
      project_igf_id: str,
      analysis_name: str,
      cellranger_group_id: str,
      cellranger_output_dir: str,
      scanpy_config: dict,
      timeout: int = 1200,
      kernel_name: str = 'python',
      allow_errors: bool = False) -> Tuple[str, str]:
  try:
    scanpy_h5ad = \
      os.path.join(
        cellranger_output_dir,
        f'scanpy_{cellranger_group_id}.h5ad')
    input_params = {
      'DATE_TAG': get_date_stamp(),
      'PROJECT_IGF_ID': project_igf_id,
      'ANALYSIS_NAME': analysis_name,
      'SAMPLE_IGF_ID': cellranger_group_id,
      'CELLRANGER_COUNT_DIR': cellranger_output_dir,
      'SCANPY_H5AD': scanpy_h5ad}
    ## update input params
    input_params.update(scanpy_config)
    input_params = {
      k.upper():v
        for k,v in input_params.items()}
    ## check paths
    singularity_image = \
      input_params.get("IMAGE_FILE")
    template_file = \
      input_params.get("TEMPLATE_FILE")
    if singularity_image is None or \
       template_file is None:
      raise KeyError(
        f"Missing template_file or image_file in design")
    check_file_path(singularity_image)
    check_file_path(template_file)
    ## TO DO: get other params from config file, if required
    ## generate notebook report
    tmp_dir = \
      get_temp_dir(use_ephemeral_space=True)
    container_bind_dir_list = [
      cellranger_output_dir,
      tmp_dir]
    if 'CELL_MARKER_LIST' in input_params:
      container_bind_dir_list.\
        append(input_params['CELL_MARKER_LIST'])
    nb = Notebook_runner(
      template_ipynb_path=template_file,
      output_dir=tmp_dir,
      input_param_map=input_params,
      container_paths=container_bind_dir_list,
      timeout=timeout,
      kernel=kernel_name,
      singularity_options=['--no-home', '-C'],
      allow_errors=allow_errors,
      use_ephemeral_space=True,
      singularity_image_path=singularity_image)
    output_notebook_path, _ = \
      nb.execute_notebook_in_singularity()
    check_file_path(scanpy_h5ad)
    check_file_path(output_notebook_path)
    return output_notebook_path, scanpy_h5ad
  except Exception as e:
    raise ValueError(
      f"Failed to run scanpy notebook, error: {e}")


## TASK
@task(
  task_id="move_single_sample_result_to_main_work_dir",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def move_single_sample_result_to_main_work_dir(
      main_work_dir: str,
      scanpy_output_dict: dict) -> dict:
  try:
    check_file_path(main_work_dir)
    cellranger_output_dir = \
      scanpy_output_dict.get("cellranger_output_dir")
    sample_group = \
      scanpy_output_dict.get("sample_group")
    target_cellranger_output_dir = \
      os.path.join(
        main_work_dir,
        os.path.basename(cellranger_output_dir))
    ## not safe to overwrite existing dir
    if os.path.exists(target_cellranger_output_dir):
      raise IOError(
        f"""cellranger output path for sample {sample_group}) already present. \
          Path: {target_cellranger_output_dir}. \
          CLEAN UP and RESTART !!!""")
    shutil.move(
      cellranger_output_dir,
      main_work_dir)
    output_dict = {
      "sample_group": sample_group,
      "cellranger_output_dir": target_cellranger_output_dir}
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


## TASK
@task(
  task_id="configure_cellranger_aggr_run",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def configure_cellranger_aggr_run() -> dict:
  try:
    cellranger_output_dict = dict()
    context = get_current_context()
    ti = context.get('ti')
    all_lazy_task_ids = \
      context['task'].\
      get_direct_relative_ids(upstream=True)
    lazy_xcom = ti.xcom_pull(task_ids=all_lazy_task_ids)
    for entry in lazy_xcom:
      sample_group = entry.get("sample_group")
      cellranger_output_dir = entry.get("cellranger_output_dir")
      if sample_group is not None and \
         cellranger_output_dir is not None:
        ## skipping failed runs
        cellranger_output_dict.update(
          {sample_group: cellranger_output_dir})
    output_dict = \
      configure_cellranger_aggr(
        run_script_template=CELLRANGER_AGGR_SCRIPT_TEMPLATE,
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



def configure_cellranger_aggr(
      run_script_template: str,
      cellranger_output_dict: dict,
      molecule_h5_name: str = 'sample_molecule_info.h5'
      ) -> dict:
  try:
    work_dir = get_temp_dir(use_ephemeral_space=True)
    cellranger_aggr_input_list = list()
    for sample_id, cellranger_output_path in cellranger_output_dict.items():
      for root,_, files in os.walk(cellranger_output_path):
        for f in files:
          if f == molecule_h5_name:
            cellranger_aggr_input_list.\
              append({
                "sample_id": sample_id,
                "molecule_h5": os.path.join(root, f)})
    output_csv_file = \
      os.path.join(work_dir, 'aggr_input.csv')
    df = pd.DataFrame(cellranger_aggr_input_list)
    df[["sample_id", "molecule_h5"]].\
      to_csv(output_csv_file, index=False)
    run_script_file = \
      os.path.join(
        work_dir,
        os.path.basename(run_script_template))
    _create_output_from_jinja_template(
      template_file=run_script_template,
      output_file=run_script_file,
      autoescape_list=['xml',],
      data=dict(
        CELLRANGER_AGGR_ID="ALL",
        CELLRANGER_AGGR_CSV=output_csv_file,
        CELLRANGER_AGGR_OUTPUT_DIR=work_dir,
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


## TASK
@task(
  task_id="run_cellranger_aggr_script",
  retry_delay=timedelta(minutes=15),
  retries=10,
  queue='hpc_8G4t72hr',
  pool='batch_job')
def run_cellranger_aggr_script(
      script_dict: dict) -> str:
  try:
    sample_name = script_dict.get('sample_name')
    run_script = script_dict.get('run_script')
    output_dir = script_dict.get('output_dir')
    try:
      stdout_file, stderr_file = \
        bash_script_wrapper(
          script_path=run_script)
    except Exception as e:
      raise ValueError(
        f"Failed to run script, Script: {run_script} for group: ALL, error file: {stderr_file}")
    ## check output dir exists
    check_file_path(output_dir)
    return output_dir
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
  task_id="merged_scanpy_report",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_32G')
def merged_scanpy_report(
      cellranger_aggr_output_dir: str,
      design_dict: dict) -> dict:
  try:
    sample_group = "ALL"
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
        cellranger_output_dir=cellranger_aggr_output_dir,
        scanpy_config=scanpy_config)
    output_dict = {
      "sample_group": sample_group,
      "cellranger_output_dir": cellranger_aggr_output_dir,
      "notebook_report": output_notebook_path,
      "scanpy_h5ad": scanpy_h5ad}
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


## TASK
@task(
  task_id="move_aggr_result_to_main_work_dir",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def move_aggr_result_to_main_work_dir(
      main_work_dir: str,
      scanpy_aggr_output_dict: dict) -> dict:
  try:
    check_file_path(main_work_dir)
    cellranger_output_dir = \
      scanpy_aggr_output_dict.get("cellranger_output_dir")
    sample_group = "ALL"
    target_cellranger_output_dir = \
      os.path.join(
        main_work_dir,
        os.path.basename(cellranger_output_dir))
    ## not safe to overwrite existing dir
    if os.path.exists(target_cellranger_output_dir):
      raise IOError(
        f"""cellranger output path for sample {sample_group}) already present. \
          Path: {target_cellranger_output_dir}. \
          CLEAN UP and RESTART !!!""")
    shutil.move(
      cellranger_output_dir,
      main_work_dir)
    return main_work_dir
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
	task_id="calculate_md5sum_for_main_work_dir",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_8G')
def calculate_md5sum_for_main_work_dir(main_work_dir: str) -> str:
  try:
    md5_sum_file = \
      calculate_md5sum_for_analysis_dir(
        dir_path=main_work_dir)
    return md5_sum_file
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
  task_id="load_cellranger_results_to_db",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def load_cellranger_results_to_db(
      main_work_dir: str,
      md5_file: str) -> str:
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
      raise ValueError('analysis_id not found in dag_run.conf')
    ## check if path exists
    check_file_path(md5_file)
    ## load data to db
    ## pipeline_name is context['task'].dag_id
    pipeline_name = context['task'].dag_id
    target_dir_path, project_igf_id, date_tag = \
    collect_analysis_dir(
      analysis_id=analysis_id,
      dag_name=pipeline_name,
      dir_path=main_work_dir,
      db_config_file=DATABASE_CONFIG_FILE,
      hpc_base_path=HPC_BASE_RAW_DATA_PATH)
    return {'target_dir_path': target_dir_path, 'date_tag': date_tag}
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
