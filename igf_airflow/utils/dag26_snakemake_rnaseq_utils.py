import os
import re
import json
import yaml
import logging
import pandas as pd
from typing import Tuple
from airflow.models import Variable
from yaml import Loader
from yaml import Dumper
from typing import Tuple
from typing import Union
from igf_data.igfdb.igfTables import (
  Pipeline,
  Pipeline_seed,
  Project,
  Analysis)
from igf_data.utils.fileutils import (
  check_file_path,
  copy_local_file)
from igf_data.utils.fileutils import (
  get_temp_dir,
  read_json_data,
  get_date_stamp_for_file_name)
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.igfdb.analysisadaptor import AnalysisAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.utils.analysis_fastq_fetch_utils import get_fastq_and_run_for_samples
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_airflow.logging.upload_log_msg import send_log_to_channels
from igf_airflow.utils.dag22_bclconvert_demult_utils import (
    _create_output_from_jinja_template,
    send_email_via_smtp)
from igf_airflow.utils.generic_airflow_utils import (
    get_project_igf_id_for_analysis,
    load_analysis_and_build_collection,
    parse_analysis_design_and_get_metadata,
    copy_analysis_to_globus_dir,
    calculate_analysis_name,
    fetch_analysis_design)
from igf_airflow.utils.generic_airflow_tasks import (
  generate_email_text_for_analysis,
  check_and_seed_analysis_pipeline,
  send_airflow_failed_logs_to_channels
)

log = logging.getLogger(__name__)

SLACK_CONF = \
  Variable.get('analysis_slack_conf',default_var=None)
MS_TEAMS_CONF = \
  Variable.get('analysis_ms_teams_conf',default_var=None)
HPC_SSH_KEY_FILE = \
  Variable.get('hpc_ssh_key_file', default_var=None)
DATABASE_CONFIG_FILE = \
  Variable.get('database_config_file', default_var=None)
HPC_BASE_RAW_DATA_PATH = \
  Variable.get('hpc_base_raw_data_path', default_var=None)
IGF_PORTAL_CONF = \
  Variable.get('igf_portal_conf', default_var=None)
HPC_FILE_LOCATION = \
  Variable.get("hpc_file_location", default_var="HPC_PROJECT")

## SNAKEMAKE
SNAKEMAKE_RUNNER_TEMPLATE = \
  Variable.get("snakemake_rnaseq_runner_template", default_var=None)
SNAKEMAKE_REPORT_TEMPLATE = \
  Variable.get("snakemake_rnaseq_report_template", default_var=None)

## EMAIL CONFIG
EMAIL_CONFIG = \
  Variable.get("email_config", default_var=None)
EMAIL_TEMPLATE = \
  Variable.get("analysis_email_template", default_var=None)

## GLOBUS
GLOBUS_ROOT_DIR = \
  Variable.get("globus_root_dir", default_var=None)


def send_email_to_user_func(**context):
  try:
    dag_run = context.get('dag_run')
    analysis_id = None
    send_email_to_user = \
      context['params'].\
      get('send_email_to_user', True)
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('analysis_id') is not None:
      analysis_id = \
        dag_run.conf.get('analysis_id')
    if analysis_id is None:
      raise ValueError(
        'analysis_id not found in dag_run.conf')
    ## get default user from email config
    email_config = \
      read_json_data(EMAIL_CONFIG)
    if isinstance(email_config, list):
      email_config = email_config[0]
    default_email_user = \
      email_config.get('username')
    if default_email_user is None:
      raise KeyError(
        f"Missing default user info in email config file {EMAIL_CONFIG}")
    ## generate email text for analysis
    email_text_file, receivers = \
      generate_email_text_for_analysis(
        analysis_id=analysis_id,
        template_path=EMAIL_TEMPLATE,
        dbconfig_file=DATABASE_CONFIG_FILE,
        default_email_user=default_email_user,
        send_email_to_user=send_email_to_user)
    ## send email to user
    send_email_via_smtp(
      sender=default_email_user,
      receivers=receivers,
      email_config_json=EMAIL_CONFIG,
      email_text_file=email_text_file)
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


def change_analysis_seed_status_func(**context):
  try:
    ## dag_run.conf should have analysis_id
    dag_run = context.get('dag_run')
    analysis_id = None
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('analysis_id') is not None:
      analysis_id = \
        dag_run.conf.get('analysis_id')
    if analysis_id is None:
      raise ValueError('analysis_id not found in dag_run.conf')
    ## pipeline_name is context['task'].dag_id
    pipeline_name = context['task'].dag_id
    ## pipeseed settings
    new_status = \
      context['params'].\
      get('new_status', '')
    no_change_status = \
      context['params'].\
      get('no_change_status', None)
    seed_table = \
      context['params'].\
        get('seed_table', None)
    ## optional, set next task if seed change is success
    next_task = \
      context['params'].\
      get('next_task', None)
    last_task = \
      context['params'].\
      get('last_task', None)
    ## change seed status
    seed_status = \
      check_and_seed_analysis_pipeline(
        analysis_id=analysis_id,
        pipeline_name=pipeline_name,
        dbconf_json_path=DATABASE_CONFIG_FILE,
        new_status=new_status,
        seed_table=seed_table,
        no_change_status=no_change_status)
    ## set next tasks
    task_list = list()
    if seed_status and \
       next_task is not None:
      task_list.append(
        next_task)
    if not seed_status and \
       last_task is not None:
      task_list.append(
        last_task)
    return task_list
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


def parse_design_and_build_inputs_for_snakemake_rnaseq(
      input_design_yaml: str,
      dbconfig_file: str,
      work_dir: str,
      config_yaml_filename: str = 'config.yaml',
      units_tsv_filename: str = 'units.tsv',
      samples_tsv_filename: str = 'samples.tsv') \
        -> Tuple[str, str, str]:
  try:
    check_file_path(dbconfig_file)
    check_file_path(work_dir)
    sample_metadata, analysis_metadata = \
      parse_analysis_design_and_get_metadata(
        input_design_yaml=input_design_yaml)
    if sample_metadata is None or \
       analysis_metadata is None:
        raise KeyError("Missing sample or analysis metadata")
    ## get sample ids from metadata
    sample_igf_id_list = \
      list(sample_metadata.keys())
    if len(sample_igf_id_list) == 0:
      raise ValueError("No sample id found in the metadata")
    ## get fastq files for all samples
    fastq_list = \
      get_fastq_and_run_for_samples(
        dbconfig_file=dbconfig_file,
        sample_igf_id_list=sample_igf_id_list)
    if len(fastq_list) == 0:
      raise ValueError(
        f"No fastq file found for samples: {input_design_yaml}")
    ## get data for sample and units tsv file
    samples_tsv_list, unites_tsv_list = \
      prepare_sample_and_units_tsv_for_snakemake_rnaseq(
        sample_metadata=sample_metadata,
        fastq_list=fastq_list)
    ## get work dir and dump snakemake input files
    if len(samples_tsv_list) == 0:
      raise ValueError("Missing samples tsv data")
    if len(unites_tsv_list) == 0:
      raise ValueError("Missing units tsv data")
    units_tsv_file = \
      os.path.join(
        work_dir,
        units_tsv_filename)
    pd.DataFrame(unites_tsv_list).\
      to_csv(
        units_tsv_file,
        sep="\t",
        index=False)
    samples_tsv_file = \
      os.path.join(
        work_dir,
        samples_tsv_filename)
    pd.DataFrame(samples_tsv_list).\
      to_csv(
        samples_tsv_file,
        sep="\t",
        index=False)
    ## dump config yaml file
    config_yaml_file = \
      os.path.join(
        work_dir,
        config_yaml_filename)
    config_yaml = dict()
    config_yaml.\
      update({
        'samples': samples_tsv_file,
        'units': units_tsv_file})
    config_yaml.\
      update(**analysis_metadata)
    output_yaml = \
      yaml.dump(
        config_yaml,
        Dumper=Dumper,
        sort_keys=False)
    with open(config_yaml_file, 'w') as fp:
      fp.write(output_yaml)
    return config_yaml_file, samples_tsv_file, units_tsv_file
  except Exception as e:
    raise ValueError(
      f"Failed to parse analysis design and generate snakemake input, error: {e}")


def prepare_sample_and_units_tsv_for_snakemake_rnaseq(
      sample_metadata: dict,
      fastq_list: list,
      sample_igf_id_key : str = 'sample_igf_id',
      file_path_key: str = 'file_path',
      fastq_group_columns: list = ['flowcell_id', 'lane_number'],
      units_tsv_columns: list = ['sra', 'adapters', 'strandedness']) \
        -> Tuple[list, list]:
  try:
    unites_tsv_list = list()
    samples_tsv_list = list()
    fq1_pattern = \
      re.compile(r'\S+_R1_001.fastq.gz')
    fq2_pattern = \
      re.compile(r'\S+_R2_001.fastq.gz')
    fastq_df = pd.DataFrame(fastq_list)
    for sample_name, sample_info in sample_metadata.items():
      samples_tsv_row = dict()
      samples_tsv_row.update({
        'sample_name': sample_name})
      ## add additional columns to samples_tsv
      for key, val in sample_info.items():
        if key not in units_tsv_columns:
          samples_tsv_row.\
            update({key: val})
      samples_tsv_list.\
        append(samples_tsv_row)
      sample_fastq = \
        fastq_df[fastq_df[sample_igf_id_key] == sample_name]
      if len(sample_fastq.index) == 0:
        raise ValueError(
          f"No fastq entry found for {sample_name}")
      check_fastq_columns = [
        f for f in sample_fastq.columns
          if f in fastq_group_columns]
      if len(check_fastq_columns) != len(fastq_group_columns):
        raise KeyError(
          f"Missing required keys in fastq list: {fastq_group_columns}")
      for (flowcell_id, lane_number), u_data in sample_fastq.groupby(fastq_group_columns):
        ## default paths are empty string
        fq1 = ''
        fq2 = ''
        ## assign fastqs to units_tsv
        for f in u_data[file_path_key].values.tolist():
          if re.match(fq1_pattern, f):
            fq1 = f
          if re.match(fq2_pattern, f):
            fq2 = f
        unites_tsv_row = dict()
        unites_tsv_row.update({
          'sample_name': sample_name,
          'unit_name': f'{flowcell_id}_{lane_number}',
          'fq1': fq1,
          'fq2': fq2})
        ## add additional columns to units_tsv
        for col_name in units_tsv_columns:
          if col_name in sample_info:
            unites_tsv_row.\
              update({
                col_name: sample_info.get(col_name)})
          else:
            unites_tsv_row.\
              update({
                col_name: ''})
        unites_tsv_list.\
          append(unites_tsv_row)
    return samples_tsv_list, unites_tsv_list
  except Exception as e:
    raise ValueError(
      f"Failed to parse analysis design and generate snakemake input, error: {e}")


def prepare_snakemake_inputs_func(**context):
  try:
    ti = context["ti"]
    snakemake_command_key = \
      context['params'].\
      get("snakemake_command_key", "snakemake_command")
    snakemake_report_key = \
      context['params'].\
      get("snakemake_report_key", "snakemake_report")
    snakemake_workdir_key = \
      context['params'].\
      get("snakemake_workdir_key", "snakemake_workdir")
    ## dag_run.conf should have analysis_id
    dag_run = context.get('dag_run')
    analysis_id = None
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('analysis_id') is not None:
      analysis_id = \
        dag_run.conf.get('analysis_id')
    if analysis_id is None:
      raise ValueError('analysis_id not found in dag_run.conf')
    ## pipeline_name is context['task'].dag_id
    pipeline_name = context['task'].dag_id
    ## get analysis design
    input_design_yaml = \
      fetch_analysis_design(
        analysis_id=analysis_id,
        pipeline_name=pipeline_name,
        dbconfig_file=DATABASE_CONFIG_FILE)
    ## prepare snakemake input files
    work_dir = \
      get_temp_dir(use_ephemeral_space=True)
    config_yaml_file, _, _ = \
      parse_design_and_build_inputs_for_snakemake_rnaseq(
        input_design_yaml=input_design_yaml,
        dbconfig_file=DATABASE_CONFIG_FILE,
        work_dir=work_dir)
    ## get project name
    project_igf_id = \
      get_project_igf_id_for_analysis(
        analysis_id=analysis_id,
        dbconfig_file=DATABASE_CONFIG_FILE)
    ## build snakemake runner script
    fastq_dir = \
      os.path.join(
        HPC_BASE_RAW_DATA_PATH,
        project_igf_id,
        'fastq')
    check_file_path(fastq_dir)
    singularity_bind_dirs = \
      f'{fastq_dir},{work_dir}'
    snakemake_runner_script = \
      os.path.join(
        work_dir,
        'snakemake_runner.sh')
    _create_output_from_jinja_template(
      template_file=SNAKEMAKE_RUNNER_TEMPLATE,
      output_file=snakemake_runner_script,
      autoescape_list=['xml',],
      data={
        "SNAKEMAKE_WORK_DIR": work_dir,
        "CONFIG_YAML_PATH": config_yaml_file,
        "SINGULARITY_BIND_DIRS": singularity_bind_dirs
      })
    ## build snakemake report script
    snakemake_report_script = \
      os.path.join(
        work_dir,
        'snakemake_report.sh')
    _create_output_from_jinja_template(
      template_file=SNAKEMAKE_REPORT_TEMPLATE,
      output_file=snakemake_report_script,
      autoescape_list=['xml',],
      data={
        "SNAKEMAKE_WORK_DIR": work_dir,
        "CONFIG_YAML_PATH": config_yaml_file
      })
    ti.xcom_push(
      key=snakemake_command_key,
      value=snakemake_runner_script)
    ti.xcom_push(
      key=snakemake_report_key,
      value=snakemake_report_script)
    ti.xcom_push(
      key=snakemake_workdir_key,
      value=work_dir)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      project_id=project_igf_id,
      comment=f"Finished pipeline run, temp snakemake work dir: {snakemake_workdir_key}",
      reaction='pass')
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


def load_analysis_to_disk_func(**context):
  try:
    ti = context["ti"]
    analysis_dir_key = \
      context['params'].\
      get("analysis_dir_key", None)
    analysis_dir_task = \
      context['params'].\
      get("analysis_dir_task", None)
    result_dir_name = \
      context['params'].\
      get("result_dir_name", None)
    collection_name = \
      context['params'].\
      get("collection_name", None)
    collection_type = \
      context['params'].\
      get("collection_type", None)
    collection_table = \
      context['params'].\
      get("collection_table", "analysis")
    reports_html = \
      context['params'].\
      get("reports_html", "reports.html")
    analysis_collection_dir_key = \
      context['params'].\
      get("analysis_collection_dir_key", "analysis_collection_dir")
    date_tag_key = \
      context['params'].\
      get("date_tag_key", "date_tag")
    ## dag_run.conf should have analysis_id
    dag_run = context.get('dag_run')
    analysis_id = None
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('analysis_id') is not None:
      analysis_id = \
        dag_run.conf.get('analysis_id')
    if analysis_id is None:
      raise ValueError('analysis_id not found in dag_run.conf')
    ## get results dir
    work_dir = \
      ti.xcom_pull(
        task_ids=analysis_dir_task,
        key=analysis_dir_key)
    if work_dir is None:
      raise ValueError("Missing analysis dir")
    result_dir = \
      os.path.join(work_dir, result_dir_name)
    check_file_path(result_dir)
    ## copy reports file to results dir
    source_report_path = \
      os.path.join(work_dir, reports_html)
    target_report_path = \
      os.path.join(result_dir, reports_html)
    if os.path.exists(source_report_path) and \
       not os.path.exists(target_report_path):
      copy_local_file(
        source_report_path,
        target_report_path)
    ## load analysis
    date_tag = get_date_stamp_for_file_name()
    if collection_type is None:
      collection_type = \
        context['task'].dag_id.upper()
    if collection_name is None:
      collection_name = \
        calculate_analysis_name(
          analysis_id=analysis_id,
          date_tag=date_tag,
          dbconfig_file=DATABASE_CONFIG_FILE)
    target_dir_path = \
      load_analysis_and_build_collection(
        collection_name=collection_name,
        collection_type=collection_type,
        collection_table=collection_table,
        dbconfig_file=DATABASE_CONFIG_FILE,
        analysis_id=analysis_id,
        pipeline_name=context['task'].dag_id,
        result_dir=result_dir,
        hpc_base_path=HPC_BASE_RAW_DATA_PATH,
        analysis_dir_prefix='analysis',
        date_tag=date_tag)
    ti.xcom_push(
      key=analysis_collection_dir_key,
      value=target_dir_path)
    ## get project name
    project_igf_id = \
      get_project_igf_id_for_analysis(
        analysis_id=analysis_id,
        dbconfig_file=DATABASE_CONFIG_FILE)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      project_id=project_igf_id,
      comment=f"Analysis finished. Output path: {target_dir_path}",
      reaction='pass')
    ti.xcom_push(
      key=date_tag_key,
      value=date_tag)
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)


def copy_analysis_to_globus_dir_func(**context):
  try:
    ti = context["ti"]
    date_tag_key = \
      context['params'].\
      get("date_tag_key", "date_tag")
    date_tag_task = \
      context['params'].\
      get("date_tag_task")
    analysis_collection_dir_key = \
      context['params'].\
      get("analysis_collection_dir_key", None)
    analysis_collection_dir_task = \
      context['params'].\
      get("analysis_collection_dir_task", None)
    analysis_dir = \
      ti.xcom_pull(
        task_ids=analysis_collection_dir_task,
        key=analysis_collection_dir_key)
    ## dag_run.conf should have analysis_id
    dag_run = context.get('dag_run')
    analysis_id = None
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('analysis_id') is not None:
      analysis_id = \
        dag_run.conf.get('analysis_id')
    if analysis_id is None:
      raise ValueError('analysis_id not found in dag_run.conf')
    ## get date tag
    date_tag = \
      ti.xcom_pull(
        task_ids=date_tag_task,
        key=date_tag_key)
    if date_tag is None:
      date_tag = get_date_stamp_for_file_name()
    target_dir_path = \
      copy_analysis_to_globus_dir(
        globus_root_dir=GLOBUS_ROOT_DIR,
        dbconfig_file=DATABASE_CONFIG_FILE,
        analysis_id=analysis_id,
        analysis_dir=analysis_dir,
        #pipeline_name=context['task'].dag_id,
        date_tag=date_tag)
    return target_dir_path
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)