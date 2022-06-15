import pandas as pd
from copy import deepcopy
import os
import re
import json
import stat
import math
import logging
import fnmatch
import subprocess
from typing import Tuple
from typing import List
from typing import Any
from airflow.models import Variable
from igf_data.illumina.runinfo_xml import RunInfo_xml
from igf_data.illumina.runparameters_xml import RunParameter_xml
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.process.singlecell_seqrun.processsinglecellsamplesheet import ProcessSingleCellDualIndexSamplesheet
from igf_data.process.singlecell_seqrun.processsinglecellsamplesheet import ProcessSingleCellSamplesheet
from igf_data.utils.box_upload import upload_file_or_dir_to_box
from igf_airflow.logging.upload_log_msg import send_log_to_channels
from igf_data.utils.fileutils import move_file
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.utils.fileutils import get_temp_dir
from igf_data.utils.fileutils import copy_remote_file
from igf_data.utils.fileutils import copy_local_file
from igf_data.utils.fileutils import check_file_path
from igf_data.utils.fileutils import read_json_data
from igf_data.utils.fileutils import get_date_stamp
from igf_data.utils.fileutils import get_date_stamp_for_file_name
from igf_data.utils.fileutils import calculate_file_checksum
from igf_data.utils.singularity_run_wrapper import execute_singuarity_cmd
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.igfdb.experimentadaptor import ExperimentAdaptor
from igf_data.igfdb.runadaptor import RunAdaptor
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_data.utils.singularity_run_wrapper import execute_singuarity_cmd
from igf_data.utils.jupyter_nbconvert_wrapper import Notebook_runner
from igf_data.utils.seqrunutils import get_seqrun_date_from_igf_id
from igf_portal.api_utils import upload_files_to_portal
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.utils.fileutils import remove_dir
from jinja2 import Template, Environment, FileSystemLoader, select_autoescape
from igf_data.utils.seqrunutils import get_seqrun_date_from_igf_id
from igf_data.process.singlecell_seqrun.mergesinglecellfastq import MergeSingleCellFastq

SLACK_CONF = Variable.get('slack_conf',default_var=None)
MS_TEAMS_CONF = Variable.get('ms_teams_conf',default_var=None)
HPC_SEQRUN_BASE_PATH = Variable.get('hpc_seqrun_path', default_var=None)
HPC_SSH_KEY_FILE = Variable.get('hpc_ssh_key_file', default_var=None)
DATABASE_CONFIG_FILE = Variable.get('database_config_file', default_var=None)
SINGLECELL_BARCODE_JSON = Variable.get('singlecell_barcode_json', default_var=None)
SINGLECELL_DUAL_BARCODE_JSON = Variable.get('singlecell_dual_barcode_json', default_var=None)
BCLCONVERT_IMAGE = Variable.get('bclconvert_image_path', default_var=None)
INTEROP_NOTEBOOK_IMAGE = Variable.get('interop_notebook_image_path', default_var=None)
BCLCONVERT_REPORT_TEMPLATE = Variable.get('bclconvert_report_template', default_var=None)
BCLCONVERT_REPORT_LIBRARY = Variable.get("bclconvert_report_library", default_var=None)
HPC_BASE_RAW_DATA_PATH = Variable.get('hpc_base_raw_data_path', default_var=None)
FASTQC_IMAGE_PATH = Variable.get('fastqc_image_path', default_var=None)
FASTQSCREEN_IMAGE_PATH = Variable.get('fastqscreen_image_path', default_var=None)
FASTQSCREEN_CONF_PATH = Variable.get('fastqscreen_conf_path', default_var=None)
FASTQSCREEN_REF_DIR = Variable.get('fastqscreen_ref_dir', default_var=None)
IGF_PORTAL_CONF = Variable.get('igf_portal_conf', default_var=None)
FTP_HOSTNAME = Variable.get('ftp_hostname', default_var=None)
FTP_USERNAME = Variable.get('ftp_username', default_var=None)
FTP_PROJECT_PATH = Variable.get('ftp_project_path', default_var=None)
QC_PAGE_TEMPLATE_DIR = Variable.get('qc_page_template_dir', default_var=None)
FASTQSCREEN_HTML_REPORT_TYPE = Variable.get('fastqscreen_html_report_type', default_var='FASTQSCREEN_HTML_REPORT')
FASTQC_HTML_REPORT_TYPE = Variable.get('fastqc_html_report_type', default_var='FASTQC_HTML_REPORT')
FASTQ_COLLECTION_TYPE = Variable.get('fastq_collection_type', default_var='demultiplexed_fastq')
MULTIQC_CONF_TEMPLATE_FILE = Variable.get("multiqc_conf_template_file", default_var=None)
MULTIQC_SINGULARITY_IMAGE = Variable.get("multiqc_singularity_image", default_var=None)
MULTIQC_HTML_REPORT_COLLECTION_TYPE = "MULTIQC_HTML_REPORT"

log = logging.getLogger(__name__)

def copy_file_to_ftp_and_load_to_db(
      ftp_server: str,
      ftp_username: str,
      base_remote_dir: str,
      project_name: str,
      dir_list: list,
      file_list: list,
      db_config_file: str,
      remote_collection_name: Any = None,
      remote_collection_type: Any = None,
      remote_collection_table: Any = None,
      remote_location: Any = None,
      ssh_key_file: Any = None) \
        -> None:
  try:
    check_file_path(db_config_file)
    ftp_file_collection_list = list()
    for file_name in file_list:
      check_file_path(file_name)
      dest_file_path = \
        os.path.join(
          base_remote_dir,
          project_name)
      if len(dir_list) > 0:
        dest_file_path = \
          os.path.join(dest_file_path, *dir_list)
      dest_file_path = \
        os.path.join(
          dest_file_path,
          os.path.basename(file_name))
      ftp_file_collection_list.append({
        'collection_name': remote_collection_name,
        'collection_type': remote_collection_type,
        'collection_table': remote_collection_table,
        'file_path': dest_file_path,
        'md5': calculate_file_checksum(file_name),
        'location': remote_location,
        'size': os.path.getsize(file_name)})
      copy_remote_file(
        source_path=file_name,
        dest_path=dest_file_path,
        destination_address=f"{ftp_username}@{ftp_server}",
        ssh_key_file=ssh_key_file)
    if remote_collection_name is not None and \
       remote_collection_type is not None and \
       remote_collection_table is not None and \
       remote_location is not None:
      load_data_raw_data_collection(
        db_config_file=db_config_file,
        collection_list=ftp_file_collection_list,
        cleanup_existing_collection=True)
  except Exception as e:
    raise ValueError(
      f"Failed to copy file to remote dir and load to db, error: {e}")


def run_multiqc(
      singularity_image_path: str,
      multiqc_report_title: str,
      multiqc_input_list: str,
      multiqc_conf_file: str,
      multiqc_param_list: list,
      multiqc_exe: str = 'multiqc'):
  try:
    bind_dir_list = list()
    with open(multiqc_input_list, 'r') as f:
      for line in f:
        bind_dir_list.append(
          line.rstrip())
    bind_dir_list.append(
      os.path.dirname(multiqc_conf_file))
    multiqc_result_dir = \
      get_temp_dir(
        use_ephemeral_space=True)
    bind_dir_list.append(
      multiqc_result_dir)
    multiqc_cmd = [
      multiqc_exe,
      '--file-list', multiqc_input_list,
      '--outdir', multiqc_result_dir,
      '--title', multiqc_report_title,
      '--config', multiqc_conf_file]
    multiqc_cmd = \
      ' '.join(multiqc_cmd)
    multiqc_cmd.extend(
      multiqc_param_list)
    execute_singuarity_cmd(
      image_path=singularity_image_path,
      command_string=multiqc_cmd,
      bind_dir_list=bind_dir_list)
    multiqc_html = None
    multiqc_data = None
    for root, _, files in os.walk(top=multiqc_result_dir):
      for file in files:
        if fnmatch.fnmatch(file, '*.html'):
          multiqc_html = \
            os.path.join(root, file)                          # get multiqc html path
        elif fnmatch.fnmatch(file, '*.zip'):
          multiqc_data = \
            os.path.join(root, file)
    return multiqc_html, multiqc_data
  except Exception as e:
    raise ValueError(
      f"Failed to run multiqc: {e}")


def multiqc_for_project_lane_index_group_func(**context):
  try:
    ti = context['ti']
    xcom_key_for_qc_file_list = \
      context['params'].\
      get("xcom_key_for_qc_file_list", "qc_file_list")
    xcom_task_for_qc_file_list = \
      context['params'].\
      get("xcom_task_for_qc_file_list")
    xcom_key_for_multiqc = \
      context['params'].\
      get("xcom_key_for_multiqc", "multiqc")
    seqrun_igf_id = \
      context['params'].\
      get('seqrun_igf_id', None)
    ## multiqc config
    tool_order_list = \
      context['params'].\
      get('tool_order_list', ['bclconvert', 'fastqc', 'fastqscreen'])
    multiqc_param_list = \
      context['params'].\
      get('multiqc_param_list', ['--zip-data-dir'])
    status_tag = \
      context['params'].\
      get('status_tag', None)
    ## fetch data about project, lane and index group
    formatted_samplesheets_list = \
      context['params'].\
      get('formatted_samplesheets', None)
    project_column = \
      context['params'].\
      get('project_column', 'project')
    project_index_column = \
      context['params'].\
      get('project_index_column', 'project_index')
    project_index = \
      context['params'].\
      get('project_index', 0)
    lane_column = \
      context['params'].\
      get('lane_column', 'lane')
    lane_index_column = \
      context['params'].\
      get('lane_index_column', 'lane_index')
    lane_index = \
      context['params'].\
      get('lane_index', 0)
    ig_index_column = \
      context['params'].\
      get('ig_index_column', 'index_group_index')
    index_group_column = \
      context['params'].\
      get('index_group_column', 'index_group')
    index_group_index = \
      context['params'].\
      get('index_group_index', 0)
    ## load formatted samplesheets and filter for project, lane and index group
    df = pd.DataFrame(formatted_samplesheets_list)
    df[ig_index_column] = \
      df[ig_index_column].astype(int)
    df[project_index_column] = \
      df[project_index_column].astype(int)
    df[lane_index_column] = \
      df[lane_index_column].astype(int)
    filt_df = \
      df[
        (df[project_index_column]==int(project_index)) &
        (df[lane_index_column]==int(lane_index)) &
        (df[ig_index_column]==int(index_group_index))]
    if len(filt_df.index) == 0 :
      raise ValueError(
        f"No samplesheet found for index group {index_group_index}")
    project_name = \
      filt_df[project_column].values.tolist()[0]
    lane_id = \
      filt_df[lane_column].values.tolist()[0]
    index_group_tag = \
      filt_df[index_group_column].values.tolist()[0]
    project_name = \
      filt_df['project_name'].values.tolist()[0]
    ## get multiqc input
    multiqc_input_list = \
      ti.xcom_pull(
        task_ids=xcom_task_for_qc_file_list,
        key=xcom_key_for_qc_file_list)
    check_file_path(multiqc_input_list)
    ## get mutiqc conf file
    temp_dir = \
      get_temp_dir(use_ephemeral_space=True)
    multiqc_conf_file = \
      os.path.join(
        temp_dir,
        'multiqc_input_file.txt')
    ## get tag name for report title
    tag_name = \
      f"{lane_id}_{index_group_tag}_{status_tag}"
    ## get current date stamp
    date_stamp = get_date_stamp()
    ## get seqrun info
    seqrun_date = \
      get_seqrun_date_from_igf_id(seqrun_igf_id)
    platform_name, flowcell_id = \
      get_platform_name_and_flowcell_id_for_seqrun(
        seqrun_igf_id=seqrun_igf_id,
        db_config_file=DATABASE_CONFIG_FILE)
    ## set multiqc report title
    multiqc_report_title = \
      f'Project:{project_name},Sequencing_date:{seqrun_date},Flowcell_lane:{flowcell_id}_{lane_id},status:{status_tag}'
    ## create config for multiqc report
    _create_output_from_jinja_template(
      template_file=MULTIQC_CONF_TEMPLATE_FILE,
      output_file=multiqc_conf_file,
      autoescape_list=['html', 'xml'],
      data=dict(
        project_igf_id=project_name,
        flowcell_id=flowcell_id,
        platform_name=platform_name,
        tag_name=tag_name,
        date_stamp=date_stamp,
        tool_order_list=tool_order_list))
    ## generate multiqc report
    multiqc_html, multiqc_data = \
      run_multiqc(
        singularity_image_path=MULTIQC_SINGULARITY_IMAGE,
        multiqc_report_title=multiqc_report_title,
        multiqc_input_list=multiqc_input_list,
        multiqc_conf_file=multiqc_conf_file,
        multiqc_param_list=multiqc_param_list)
    # ti.xcom_push(
    #   key=xcom_key_for_multiqc,
    #   value={
    #     "project_index": project_index,
    #     "lane_index": lane_index,
    #     "index_group_index": index_group_index,
    #     "multiqc_html": multiqc_html,
    #     "multiqc_data": multiqc_data,
    #     "xcom_key_for_qc_file_list": multiqc_input_list})
    ## load multiqc report to collection table
    dir_list = [
      project_name,
      'fastq_multiqc',
      seqrun_date,
      flowcell_id,
      lane_id,
      index_group_tag,
      status_tag]
    multiqc_collection_name = \
      f"{project_name}_{flowcell_id}_{lane_id}_{index_group_tag}_{status_tag}"
    multiqc_collection_list = [{
      'collection_name': multiqc_collection_name,
      'dir_list': dir_list,
      'file_list': [multiqc_html]}]
    _ = \
      load_raw_files_to_db_and_disk(
        db_config_file=DATABASE_CONFIG_FILE,
        collection_type=MULTIQC_HTML_REPORT_COLLECTION_TYPE,
        collection_table="file",
        base_data_path=HPC_BASE_RAW_DATA_PATH,
        file_location='HPC_PROJECT',
        replace_existing_file=True,
        cleanup_existing_collection=True,
        collection_list=multiqc_collection_list)
    multiqc_data_dict = {
      "file_list": [multiqc_html],
      "project_igf_id": project_name,
      "dir_list": [flowcell_id, lane_id, index_group_tag, status_tag],
      "collection_name": multiqc_collection_name,
      "collection_table": "file"}
    ti.xcom_push(
      key=xcom_key_for_multiqc,
      value=multiqc_data_dict)
  except Exception as e:
    log.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def collect_qc_reports_for_samples_func(**context):
  try:
    ti = context['ti']
    xcom_key_for_bclconvert_output = \
      context['params'].\
      get("xcom_key_for_bclconvert_output", "bclconvert_output")
    bclconvert_task_prefix = \
      context['params'].\
      get("bclconvert_task_prefix", "bclconvert_")
    fastqc_task_prefix = \
      context['params'].\
      get("fastqc_task_prefix", "fastqc_")
    fastq_screen_task_prefix = \
      context['params'].\
      get("fastq_screen_task_prefix", "fastq_screen_")
    xcom_key_for_fastqc_output = \
      context['params'].\
      get("xcom_key_for_fastqc_output", "fastqc_output")
    xcom_key_for_fastq_screen_output = \
      context['params'].\
      get("xcom_key_for_fastq_screen_output", "fastq_screen_output")
    xcom_key_for_qc_file_list = \
      context['params'].\
      get("xcom_key_for_qc_file_list", "qc_file_list")
    all_task_ids = \
      context['task'].\
      get_direct_relative_ids(upstream=True)
    qc_output_list = list()
    for task_name in all_task_ids:
      if task_name.startswith(bclconvert_task_prefix):
        bclconvert_output = \
          ti.xcom_pull(
            task_ids=task_name,
            key=xcom_key_for_bclconvert_output)
        qc_output_list.append(bclconvert_output)
      elif task_name.startswith(fastqc_task_prefix):
        fastqc_output = \
          ti.xcom_pull(
            task_ids=task_name,
            key=xcom_key_for_fastqc_output)
        qc_output_list.append(fastqc_output)
      elif task_name.startswith(fastq_screen_task_prefix):
        fastq_screen_output = \
          ti.xcom_pull(
            task_ids=task_name,
            key=xcom_key_for_fastq_screen_output)
        qc_output_list.append(fastq_screen_output)
    work_dir = \
      get_temp_dir(use_ephemeral_space=True)
    qc_output_file = \
      os.path.join(work_dir, "qc_output.txt")
    with open(qc_output_file, "w") as fp:
      fp.write('\n'.join(qc_output_list))
    ti.xcom_push(
      key=xcom_key_for_qc_file_list,
      value=qc_output_file)
  except Exception as e:
    log.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise

def run_fastqScreen(
      fastqscreen_image_path: str,
      fastq_path: str,
      output_dir: str,
      fastqscreen_conf: str,
      fastqscreen_ref_dir: str,
      fastqscreen_exe: str = 'fastq_screen',
      fastqscreen_options: tuple = (
        '--aligner bowtie2',
        '--force',
        '--quiet',
        '--subset 100000',
        '--threads 1')) \
        -> list:
  try:
    check_file_path(fastqscreen_image_path)
    check_file_path(fastq_path)
    check_file_path(output_dir)
    check_file_path(fastqscreen_conf)
    check_file_path(fastqscreen_ref_dir)
    temp_dir = get_temp_dir()
    fastqscreen_cmd = [
      fastqscreen_exe,
      '-conf', fastqscreen_conf,
      '--outdir', temp_dir]
    fastqscreen_cmd.extend(fastqscreen_options)
    fastqscreen_cmd.append(fastq_path)
    bind_dir_list = [
      temp_dir,
      fastqscreen_ref_dir,
      os.path.dirname(fastq_path),
      os.path.dirname(fastqscreen_conf)]
    execute_singuarity_cmd(
      image_path=fastqscreen_image_path,
      command_string=' '.join(fastqscreen_cmd),
      bind_dir_list=bind_dir_list)
    output_file_list = list()
    for file_path in os.listdir(temp_dir):
      if file_path.endswith('.txt') or \
         file_path.endswith('.html'):
        source_path = \
          os.path.join(temp_dir, file_path)
        dest_path = \
          os.path.join(output_dir, file_path)
        copy_local_file(source_path, dest_path, force=True)
        output_file_list.append(dest_path)
    return output_file_list
  except Exception as e:
    raise ValueError(
      f"Failed to run fastqscreen, error: {e}")


def run_fastqc(
    fastqc_image_path: str,
    fastq_path: str,
    output_dir: str,
    fastqc_exe: str = 'fastqc',
    fastqc_options: tuple = ('-q', '--noextract', '-ffastq', '-k7', '-t1')) \
      -> list:
  try:
    check_file_path(fastq_path)
    check_file_path(output_dir)
    check_file_path(fastqc_image_path)
    temp_dir = get_temp_dir()
    if isinstance(fastqc_options, tuple):
      fastqc_options = list(fastqc_options)
    fastqc_cmd = [
      fastqc_exe,
      '-o', temp_dir,
      '-d', temp_dir]
    fastqc_cmd.extend(fastqc_options)                                           # add additional parameters
    fastqc_cmd.append(fastq_path)
    bind_dir_list = [
      temp_dir,
      os.path.dirname(fastq_path)]
    execute_singuarity_cmd(
      image_path=fastqc_image_path,
      command_string=' '.join(fastqc_cmd),
      bind_dir_list=bind_dir_list)
    fastqc_zip = list()
    fastqc_html = list()
    for files in os.listdir(temp_dir):
      if files.endswith('.zip'):
        fastqc_zip.append(os.path.join(temp_dir, files))
      elif files.endswith('.html'):
        fastqc_html.append(os.path.join(temp_dir, files))
    if len(fastqc_html) == 0:
      raise ValueError("No fastqc html report found")
    output_file_list = list()
    for html_file in fastqc_html:
      dest_file = \
        os.path.join(
          output_dir,
          os.path.basename(html_file))
      copy_local_file(
        source_path=html_file,
        destination_path=dest_file)
      output_file_list.append(dest_file)
    for zip_file in fastqc_zip:
      dest_file = \
        os.path.join(
          output_dir,
          os.path.basename(zip_file))
      copy_local_file(
        source_path=zip_file,
        destination_path=dest_file)
      output_file_list.append(dest_file)
    return output_file_list
  except Exception as e:
    raise ValueError(
      f"Failed to run fastqc for {fastq_path}")


def fastqscreen_run_wrapper_for_known_samples_func(**context):
  try:
    ti = context['ti']
    # xcom_key_for_bclconvert_output = \
    #   context['params'].\
    #   get("xcom_key_for_bclconvert_output", "bclconvert_output")
    # xcom_task_for_bclconvert_output = \
    #   context['params'].\
    #   get("xcom_task_for_bclconvert_output")
    xcom_key_for_collection_group = \
      context['params'].\
      get("xcom_key_for_collection_group", "collection_group")
    xcom_task_for_collection_group = \
      context['params'].\
      get("xcom_task_for_collection_group")
    xcom_key_for_fastq_screen_output = \
      context['params'].\
      get("xcom_key_for_fastq_screen_output", "fastq_screen_output")
    xcom_key_for_fastq_screen_collection = \
      context['params'].\
      get("xcom_key_for_fastq_screen_collection", "fastq_screen_collection")
    fastqscreen_collection_type = FASTQSCREEN_HTML_REPORT_TYPE
    collection_table = 'run'
    # bclconvert_output = \
    #   ti.xcom_pull(
    #     task_ids=xcom_task_for_bclconvert_output,
    #     key=xcom_key_for_bclconvert_output)
    collection_group = \
      ti.xcom_pull(
        task_ids=xcom_task_for_collection_group,
        key=xcom_key_for_collection_group)
    # fastqscreen_temp_output_path = \
    #   os.path.join(
    #     bclconvert_output,
    #     'fastqscreen_dir')
    # os.makedirs(
    #   fastqscreen_temp_output_path,
    #   exist_ok=True)
    fastqscreen_collection_list = list()
    work_dir = \
      get_temp_dir(
        use_ephemeral_space=True)
    for entry in collection_group:
      collection_name = entry.get('collection_name')
      dir_list = entry.get('dir_list')
      file_list = entry.get('file_list')
      ## RUN FASTQACREEN for the file
      fastq_screen_output_list = list()
      for fastq_file_entry in file_list:
        fastq_file = \
          fastq_file_entry.get('file_path')
        output_fastqc_list = \
          run_fastqScreen(
            fastqscreen_image_path=FASTQSCREEN_IMAGE_PATH,
            fastqscreen_conf=FASTQSCREEN_CONF_PATH,
            fastqscreen_ref_dir=FASTQSCREEN_REF_DIR,
            fastq_path=fastq_file,
            output_dir=work_dir)
        for file_entry in output_fastqc_list:
          # dest_path = \
          #   os.path.join(
          #     fastqscreen_temp_output_path,
          #     os.path.basename(file_entry))
          # copy_local_file(
          #   file_entry,
          #   dest_path,
          #   force=True)
          if file_entry.endswith('.html'):
            fastq_screen_output_list.append({
              'file_path': file_entry,
              'md5': calculate_file_checksum(file_entry)})
      ## LOAD FASTQC REPORT TO DB
      dir_list = [
        f if f != 'fastq' else 'fastqscreen'
          for f in dir_list]
      fastqscreen_collection_list.append({
        'collection_name': collection_name,
        'dir_list': dir_list,
        'file_list': fastq_screen_output_list})
    file_collection_list = \
      load_raw_files_to_db_and_disk(
        db_config_file=DATABASE_CONFIG_FILE,
        collection_type=fastqscreen_collection_type,
        collection_table=collection_table,
        base_data_path= HPC_BASE_RAW_DATA_PATH,
        file_location='HPC_PROJECT',
        replace_existing_file=True,
        cleanup_existing_collection=True,
        collection_list=fastqscreen_collection_list)
    ti.xcom_push(
      key=xcom_key_for_fastq_screen_output,
      value=work_dir)
    ti.xcom_push(
      key=xcom_key_for_fastq_screen_collection,
      value=file_collection_list)
  except Exception as e:
    log.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def fastqc_run_wrapper_for_known_samples_func(**context):
  try:
    # TO DO
    # * get fastq file and sample name from xcom
    # * get fastqc temp output path from xcom
    # * get fastqc image
    # * run fastqc
    # * collect report per sample
    # * load report to disk and db
    # * copy fastqc results to temp output path
    ti = context['ti']
    # xcom_key_for_bclconvert_output = \
    #   context['params'].\
    #   get("xcom_key_for_bclconvert_output", "bclconvert_output")
    # xcom_task_for_bclconvert_output = \
    #   context['params'].\
    #   get("xcom_task_for_bclconvert_output")
    xcom_key_for_collection_group = \
      context['params'].\
      get("xcom_key_for_collection_group", "collection_group")
    xcom_task_for_collection_group = \
      context['params'].\
      get("xcom_task_for_collection_group")
    xcom_key_for_fastqc_output = \
      context['params'].\
      get('xcom_key_for_fastqc_output', 'fastqc_output')
    xcom_key_for_fastqc_collection = \
      context['params'].\
      get('xcom_key_for_fastqc_collection', 'fastqc_collection')
    fastqc_collection_type = FASTQC_HTML_REPORT_TYPE
    collection_table = 'run'
    # bclconvert_output = \
    #   ti.xcom_pull(
    #     task_ids=xcom_task_for_bclconvert_output,
    #     key=xcom_key_for_bclconvert_output)
    collection_group = \
      ti.xcom_pull(
        task_ids=xcom_task_for_collection_group,
        key=xcom_key_for_collection_group)
    # fastqc_temp_output_path = \
    #   os.path.join(bclconvert_output, 'fastqc_dir')
    # os.makedirs(fastqc_temp_output_path, exist_ok=True)
    fastqc_collection_list = list()
    work_dir = \
      get_temp_dir(use_ephemeral_space=True)
    # all_fastqc_output_list = list()
    for entry in collection_group:
      collection_name = entry.get('collection_name')
      dir_list = entry.get('dir_list')
      file_list = entry.get('file_list')
      ## RUN FASTQC for the file
      fastq_output_list = list()
      for fastq_file_entry in file_list:
        fastq_file = fastq_file_entry.get('file_path')
        output_fastqc_list = \
          run_fastqc(
            fastqc_image_path=FASTQC_IMAGE_PATH,
            fastq_path=fastq_file,
            output_dir=work_dir)
        for file_entry in output_fastqc_list:
          # dest_path = \
          #   os.path.join(
          #     fastqc_temp_output_path,
          #     os.path.basename(file_entry))
          # copy_local_file(
          #   file_entry,
          #   dest_path, force=True)
          if file_entry.endswith('.html'):
            fastq_output_list.append({
              'file_path': file_entry,
              'md5': calculate_file_checksum(file_entry)})
      ## LOAD FASTQC REPORT TO DB
      dir_list = [
        f if f != 'fastq' else 'fastqc'
          for f in dir_list]
      fastqc_collection_list.append({
        'collection_name': collection_name,
        'dir_list': dir_list,
        'file_list': fastq_output_list})
    file_collection_list = \
      load_raw_files_to_db_and_disk(
        db_config_file=DATABASE_CONFIG_FILE,
        collection_type=fastqc_collection_type,
        collection_table=collection_table,
        base_data_path= HPC_BASE_RAW_DATA_PATH,
        file_location='HPC_PROJECT',
        replace_existing_file=True,
        cleanup_existing_collection=True,
        collection_list=fastqc_collection_list)
    ti.xcom_push(
      key=xcom_key_for_fastqc_output,
      value=work_dir)
    ti.xcom_push(
      key=xcom_key_for_fastqc_collection,
      value=file_collection_list)
  except Exception as e:
    log.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def get_platform_name_and_flowcell_id_for_seqrun(
      seqrun_igf_id: str,
      db_config_file: str) \
        -> Tuple[str, str]:
    try:
      check_file_path(db_config_file)
      dbparams = read_dbconf_json(db_config_file)
      sr = SeqrunAdaptor(**dbparams)
      sr.start_session()
      platform_name = \
        sr.fetch_platform_info_for_seqrun(
          seqrun_igf_id=seqrun_igf_id)
      seqrun = \
        sr.fetch_seqrun_records_igf_id(
          seqrun_igf_id=seqrun_igf_id)
      return platform_name, seqrun.flowcell_id
    except Exception as e:
      raise ValueError(
        f"Failed to get platform name and flowcell id for seqrun {seqrun_igf_id}, error: {e}")


def get_project_id_samples_list_from_db(
      sample_igf_id_list: list,
      db_config_file: str) \
        -> dict:
    try:
      check_file_path(db_config_file)
      project_sample_dict = dict()
      dbparams = read_dbconf_json(db_config_file)
      sa = SampleAdaptor(**dbparams)
      sa.start_session()
      for sample_id in sample_igf_id_list:
        project_id = \
          sa.fetch_sample_project(sample_igf_id=sample_id)
        if project_id is None:
          raise ValueError(
            f"Failed to get project id for sample {sample_id}")
        project_sample_dict.\
          update({sample_id: project_id})
      sa.close_session()
      return project_sample_dict
    except Exception as e:
      raise ValueError(
        f"Failed to get project id and samples list from db, error: {e}")


def copy_or_replace_file_to_disk_and_change_permission(
      source_path: str,
      destination_path: str,
      replace_existing_file: bool = False,
      make_file_and_dir_read_only : bool = True) \
      -> None:
  try:
    if os.path.exists(destination_path):
      if not replace_existing_file:
        raise ValueError(
          f"File {destination_path} already exists. Set replace_existing_file to True")
      else:
        # add write permission for user
        os.chmod(destination_path, stat.S_IWUSR)
        # os.chmod(
        #   os.path.dirname(destination_path),
        #   stat.S_IWUSR |
        #   stat.S_IXUSR)
    # if os.path.exists(os.path.dirname(destination_path)):
    #   os.chmod(
    #       os.path.dirname(destination_path),
    #       stat.S_IWUSR |
    #       stat.S_IXUSR)
    copy_local_file(
      source_path,
      destination_path,
      force=replace_existing_file)
    if make_file_and_dir_read_only:
      if os.path.isdir(destination_path):
        # make dir read only
        os.chmod(
          destination_path,
          stat.S_IRUSR |
          stat.S_IRGRP |
          stat.S_IXUSR |
          stat.S_IXGRP)
      elif os.path.isfile(destination_path):
        # make file read only
        os.chmod(
          destination_path,
          stat.S_IRUSR |
          stat.S_IRGRP)
        # make dir read only
        # os.chmod(
        #   os.path.dirname(destination_path),
        #   stat.S_IRUSR |
        #   stat.S_IRGRP |
        #   stat.S_IXUSR |
        #   stat.S_IXGRP)
  except Exception as e:
    raise ValueError(
      f"Failed to copy file to new path, error: {e}")


def load_data_raw_data_collection(
      db_config_file: str,
      collection_list: list,
      collection_name_key: str = 'collection_name',
      collection_type_key: str = 'collection_type',
      collection_table_key: str = 'collection_table',
      file_path_key: str = 'file_path',
      md5_key: str = 'md5',
      size_key: str = 'size',
      location_key: str = 'location',
      cleanup_existing_collection: bool = False) \
        -> None:
    try:
      check_file_path(db_config_file)
      dbparam = read_dbconf_json(db_config_file)
      ca = CollectionAdaptor(**dbparam)
      ca.start_session()
      fa = FileAdaptor(**{'session': ca.session})
      try:
        collection_data_list = list()
        collection_df = pd.DataFrame(collection_list)
        if collection_name_key not in collection_df.columns or \
           collection_type_key not in collection_df.columns or \
           file_path_key not in collection_df.columns:
          raise KeyError("Missing key in collection entry")
        collection_lookup_columns = [
          collection_name_key,
          collection_type_key]
        unique_collections_list = \
          collection_df[collection_lookup_columns].\
          drop_duplicates().\
          to_dict(orient='records')
        for collection_entry in unique_collections_list:
          collection_name = collection_entry[collection_name_key]
          collection_type = collection_entry[collection_type_key]
          collection_exists = \
            ca.get_collection_files(
              collection_name=collection_name,
              collection_type=collection_type)
          if len(collection_exists.index) > 0 and \
             cleanup_existing_collection:
            remove_data = [{
              "name": collection_name,
              "type": collection_type }]
            ca.remove_collection_group_info(
              data=remove_data,
              autosave=False)
        unique_files_list = \
          collection_df[file_path_key].\
          drop_duplicates().\
          values.\
          tolist()
        for file_path in unique_files_list:
          file_exists = \
            fa.check_file_records_file_path(
              file_path=file_path)
          if file_exists:
            if cleanup_existing_collection:
              fa.remove_file_data_for_file_path(
                file_path=file_path,
                remove_file=False,
                autosave=False)
            else:
              raise ValueError(
                f"File {file_path} already exists in database")
        for entry in collection_list:
          if collection_name_key not in entry or \
             collection_type_key not in entry or \
             file_path_key not in entry:
            raise KeyError("Missing key in collection entry")
          collection_name = entry[collection_name_key]
          collection_type = entry[collection_type_key]
          collection_table = entry[collection_table_key]
          file_path = entry[file_path_key]
          md5 = entry.get(md5_key, None)
          size = entry.get(size_key, None)
          location = entry.get(location_key, None)
          if not os.path.exists(file_path):
            raise ValueError(
              f"File {file_path} does not exist")
          collection_data = {
            'name': collection_name,
            'type': collection_type,
            'table': collection_table,
            'file_path': file_path}
          if md5 is not None:
            collection_data.update({'md5': md5})
          if size is not None:
            collection_data.update({'size': size})
          if location is not None:
            collection_data.update({'location': location})
          collection_data_list.append(collection_data)
        if len(collection_data_list) > 0:
          ca.load_file_and_create_collection(
            data=collection_data_list,
            calculate_file_size_and_md5=False,
            autosave=False)
        else:
          raise ValueError("No collection data to load")
        ca.commit_session()
        ca.close_session()
      except:
        ca.rollback_session()
        ca.close_session()
        raise
    except Exception as e:
      raise ValueError(
        f"Failed to load collection, error: {e}")


def load_raw_files_to_db_and_disk(
      db_config_file: str,
      collection_type: str,
      collection_table: str,
      base_data_path: str,
      file_location: str,
      replace_existing_file: bool,
      cleanup_existing_collection: bool,
      collection_list: list,
      collection_name_key: str = 'collection_name',
      dir_list_key: str = 'dir_list',
      file_list_key: str = 'file_list') \
        -> list:
    try:
      ## TO DO:
      # * get collection name from list
      # * get files from collection_list
      # * get file size
      # * calculate destination path
      # * create dir and cd to dest path
      # * replace existing file if true
      # * copy file to destination path
      # * change dir permission to read and execute for group
      # * change file path permission to read only for group
      # * build file collection
      # * clean up collection if exists and cleanup_existing_collection is True
      # * append file to collection if cleanup_existing_collection is False
      # * return collected file list group [{'collection_name': '', file_list': []}]
      check_file_path(db_config_file)
      ## get collection name and file list from collection_list
      file_collection_list = list()
      for entry in collection_list:
        if collection_name_key not in entry:
          raise KeyError(
            f"{collection_name_key} key not found in collection list")
        if file_list_key not in entry:
          raise KeyError(
            f"{file_list_key} key not found in collection list")
        if dir_list_key not in entry:
          raise KeyError(
            f"{dir_list_key} key not found in collection list")
        collection_name = \
          entry.get(collection_name_key)
        file_list = \
          entry.get(file_list_key)
        dir_list = \
          entry.get(dir_list_key)
        if not isinstance(dir_list, list):
          raise TypeError("dir_list must be a list")
        if len(file_list) == 0:
          raise ValueError(
            f"No files found in collection {collection_name}")
        for file_list_entry in file_list:
          file_path = file_list_entry.get('file_path')
          file_md5 = file_list_entry.get('md5')
          check_file_path(file_path)
          file_size = os.path.getsize(file_path)
          ## get destination path
          if len(dir_list) == 0:
            destination_path = \
              os.path.join(
                base_data_path,
                os.path.basename(file_path))
          else:
            dir_list = [
              str(f) for f in dir_list]
            destination_path = \
              os.path.join(
                base_data_path,
                *dir_list,
                os.path.basename(file_path))
          ## add to collec list
          file_collection_list.append({
            'collection_name': collection_name,
            'collection_type': collection_type,
            'collection_table': collection_table,
            'file_path': destination_path,
            'md5': file_md5,
            'location': file_location,
            'size': file_size})
          ## check existing path and copy file
          ## TO DO
          copy_or_replace_file_to_disk_and_change_permission(
            source_path=file_path,
            destination_path=destination_path,
            replace_existing_file=replace_existing_file,
            make_file_and_dir_read_only=True)
      ## clean up existing collection and load data to db
      # TO DO
      load_data_raw_data_collection(
        db_config_file=db_config_file,
        collection_list=file_collection_list,
        cleanup_existing_collection=cleanup_existing_collection)
      return file_collection_list
    except Exception as e:
      raise ValueError(
        f"Failed to load raw files to db, error: {e}")


def register_experiment_and_runs_to_db(
      db_config_file: str,
      seqrun_id: str,
      lane_id: int,
      index_group: str,
      sample_group: list) \
        -> list:
    try:
      check_file_path(db_config_file)
      (platform_name, flowcell_id) = \
        get_platform_name_and_flowcell_id_for_seqrun(
          seqrun_igf_id=seqrun_id,
          db_config_file=db_config_file)
      seqrun_date = \
        get_seqrun_date_from_igf_id(seqrun_id)
      sample_group_with_run_id = list()
      sample_id_list = list()
      exp_data = list()
      run_data = list()
      ## LOOP 1
      for entry in sample_group:
        if 'sample_id' not in entry:
          raise KeyError("Missing key sample_id")
        sample_id = entry.get('sample_id')
        sample_id_list.append(sample_id)
      project_sample_dict = \
        get_project_id_samples_list_from_db(
          sample_igf_id_list=sample_id_list,
          db_config_file=db_config_file)
      ## LOOP 2
      for entry in sample_group:
        if 'sample_id' not in entry:
          raise KeyError("Missing key sample_id")
        sample_id = entry.get('sample_id')
        project_id = project_sample_dict.get(sample_id)
        if project_id is None:
          raise ValueError(
            f"Failed to get project id for sample {sample_id}")
        # set library id
        library_id = sample_id
        # calcaulate experiment id
        experiment_id = \
          f'{library_id}_{platform_name}'
        # calculate run id
        run_igf_id = \
          f'{experiment_id}_{flowcell_id}_{lane_id}'
        library_layout = 'SINGLE'
        for fastq in entry.get('fastq_list'):
          if fastq.endswith('_R2_001.fastq.gz'):
            library_layout = 'PAIRED'
        #sample_group_with_run_id.append({
        #  'project_igf_id': project_id,
        #  'sample_igf_id': sample_id,
        #  'library_id': library_id,
        #  'experiment_igf_id': experiment_id,
        #  'run_igf_id': run_igf_id,
        #  'library_layout': library_layout,
        #  'lane_number': lane_id,
        #  'fastq_list': entry.get('fastq_list')
        #})
        sample_group_with_run_id.append({
          'collection_name': run_igf_id,
          'dir_list': [
            project_id,
            'fastq',
            seqrun_date,
            flowcell_id,
            str(lane_id),
            index_group,
            sample_id],
          'file_list':[{
            'file_path': file_name,
            'md5': file_md5}
              for file_name, file_md5 in entry.get('fastq_list').items()]
          })
        exp_data.append({
          'project_igf_id': project_id,
          'sample_igf_id': sample_id,
          'library_name': library_id,
          'experiment_igf_id': experiment_id,
          'library_layout': library_layout
        })
        run_data.append({
          'experiment_igf_id': experiment_id,
          'run_igf_id': run_igf_id,
          'lane_number': str(lane_id),
          'seqrun_igf_id': seqrun_id
        })
      ## register exp and run data
      filtered_exp_data = list()
      filtered_run_data = list()
      dbparams = read_dbconf_json(db_config_file)
      base = BaseAdaptor(**dbparams)
      base.start_session()
      ea = \
        ExperimentAdaptor(**{'session': base.session})
      ra = \
        RunAdaptor(**{'session': base.session})
      for exp_entry in exp_data:
        if 'experiment_igf_id' not in exp_entry:
          raise KeyError("Missing key experiment_igf_id")
        experiment_exists = \
          ea.check_experiment_records_id(
            exp_entry.get('experiment_igf_id'))
        if not experiment_exists:
          filtered_exp_data.append(exp_entry)
      for run_entry in run_data:
        if 'run_igf_id' not in run_entry:
          raise KeyError("Missing key run_igf_id")
        run_exists = \
          ra.check_run_records_igf_id(
            run_entry.get('run_igf_id'))
        if not run_exists:
          filtered_run_data.append(run_entry)
      try:
        if len(filtered_exp_data) > 0:
          ea.store_project_and_attribute_data(
            data=filtered_exp_data,
            autosave=False)
        base.session.flush()
        if len(filtered_run_data) > 0:
          ra.store_run_and_attribute_data(
            data=filtered_run_data,
            autosave=False)
        base.session.flush()
        base.session.commit()
        base.close_session()
      except:
        base.session.rollback()
        base.close_session()
        raise
      return sample_group_with_run_id
    except Exception as e:
      raise ValueError(
        f"Failed to register experiment and runs, error: {e}")


def load_fastq_and_qc_to_db_func(**context):
  try:
    ti = context['ti']
    seqrun_igf_id = \
      context['params'].\
      get("seqrun_igf_id", None)
    formatted_samplesheets_list = \
      context['params'].\
      get("formatted_samplesheets", None)
    xcom_key_for_checksum_sample_group = \
      context['params'].\
      get("xcom_key_for_checksum_sample_group", "checksum_sample_group")
    xcom_task_for_checksum_sample_group = \
      context['params'].\
      get("xcom_task_for_checksum_sample_group")
    #lane_id = context['params'].get('lane_id')
    # xcom_key = \
    #   context['params'].\
    #   get('xcom_key', 'formatted_samplesheets')
    # xcom_task = \
    #   context['params'].\
    #   get('xcom_task', 'format_and_split_samplesheet')
    xcom_key_for_collection_group = \
      context['params'].\
      get("xcom_key_for_collection_group", "collection_group")
    project_index_column = \
      context['params'].\
      get('project_index_column', 'project_index')
    project_index = \
      context['params'].\
      get('project_index', 0)
    lane_index_column = \
      context['params'].\
      get('lane_index_column', 'lane_index')
    lane_index = \
      context['params'].\
      get('lane_index', 0)
    ig_index_column = \
      context['params'].\
      get('ig_index_column', 'index_group_index')
    ig_index = \
      context['params'].\
      get('ig_index', 0)
    index_group_column = \
      context['params'].\
      get('index_group_column', 'index_group')
    if project_index == 0 or \
       lane_index == 0 or \
       ig_index == 0:
      raise ValueError(
        "project_index, lane_index or ig_index is not set")
    # formatted_samplesheets_list = \
    #   ti.xcom_pull(task_ids=xcom_task, key=xcom_key)
    df = pd.DataFrame(formatted_samplesheets_list)
    if project_index_column not in df.columns or \
        lane_index_column not in df.columns or \
        ig_index_column not in df.columns:
      raise KeyError(""""
        project_index_column, lane_index_column or
        ig_index_column is not found""")
    ig_df = \
      df[
        (df[project_index_column]==project_index) &
        (df[lane_index_column]==lane_index) &
        (df[ig_index_column]==ig_index)]
    if len(ig_df.index) == 0:
      raise ValueError(
        f"No index group found for project {project_index}, lane {lane_index}, ig {ig_index}")
    index_group = \
      ig_df[index_group_column].values.tolist()[0]
    checksum_sample_group = \
      ti.xcom_pull(
        task_ids=xcom_task_for_checksum_sample_group,
        key=xcom_key_for_checksum_sample_group)
    ## TO DO: fix lane_id
    lane_id = lane_index
    # dag_run = context.get('dag_run')
    # if dag_run is None or \
    #    dag_run.conf is None or \
    #    dag_run.conf.get('seqrun_id') is None:
    #   raise ValueError('Missing seqrun_id in dag_run.conf')
    # seqrun_id = dag_run.conf.get('seqrun_id')
    ## To DO:
    #  for each sample in the sample_group
    #    * get sample_id
    #    * get calculate experiment and run id
    #    * get check library_layout based on R1 and R2 reads
    #    * register experiment and run ids if they are not present
    #    * load fastqs with the run id as collection name
    #    * do these operations in batch mode
    if seqrun_igf_id is None:
      raise ValueError("Missing seqrun_igf_id")
    fastq_collection_list = \
      register_experiment_and_runs_to_db(
        db_config_file=DATABASE_CONFIG_FILE,
        seqrun_id=seqrun_igf_id,
        lane_id=lane_id,
        index_group=index_group,
        sample_group=checksum_sample_group)
    ## fastq_collection_list
    #  * [{
    #     'collection_name': '',
    #     'dir_list': [
    #       project_igf_id,
    #       fastq,
    #       run_date,
    #       flowcell_id,
    #       lane_id,
    #       index_group_id,
    #       sample_id],
    #     'file_list': [{'file_name': fastq_files, 'md5': md5}] }]
    file_collection_list = \
      load_raw_files_to_db_and_disk(
        db_config_file=DATABASE_CONFIG_FILE,
        collection_type=FASTQ_COLLECTION_TYPE,
        collection_table='run',
        base_data_path= HPC_BASE_RAW_DATA_PATH,
        file_location='HPC_PROJECT',
        collection_list=fastq_collection_list,
        replace_existing_file=True,
        cleanup_existing_collection=True)
    ti.xcom_push(
      key=xcom_key_for_collection_group,
      value=fastq_collection_list)
  except Exception as e:
    log.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def get_sample_info_from_sample_group(
      worker_index: int,
      sample_group: list) \
        -> list:
  try:
    if len(sample_group) == 0:
      raise ValueError("sample_group is empty")
    df = pd.DataFrame(sample_group)
    if 'worker_index' not in df.columns or \
       'sample_ids' not in df.columns:
      raise KeyError(
        "worker_index or sample_ids is not in sample_group")
    df['worker_index'] = \
      df['worker_index'].astype(int)
    filt_df = df[df['worker_index'] == int(worker_index)]
    if len(filt_df.index) == 0:
      raise ValueError(
        f"worker_index {worker_index} is not in sample_group")
    sample_ids = \
      filt_df['sample_ids'].values.tolist()[0]
    if len(sample_ids) == 0:
      raise ValueError("sample_ids is empty")
    fastq_files_list = list()
    for entry in sample_ids:
      sample_name = entry.get('sample_id')
      fastq_list = entry.get('fastq_list')
      fastq_files_list.append({
        'sample_id': sample_name,
        'fastq_list': fastq_list})
    return fastq_files_list
  except Exception as e:
    raise ValueError(
      f"Failed to get sample info from sample group, error: {e}")


def get_checksum_for_sample_group_fastq_files(
      sample_group: list) \
        -> list:
  try:
    if len(sample_group) == 0:
      raise ValueError("sample_group is empty")
    check_sum_sample_group = list()
    for entry in sample_group:
      sample_id = entry.get("sample_id")
      fastq_list = entry.get("fastq_list")
      fastq_dict = dict()
      for fastq_file in fastq_list:
        fastq_md5 = \
          calculate_file_checksum(fastq_file)
        fastq_dict.update({
          fastq_file: fastq_md5})
      check_sum_sample_group.append({
        'sample_id': sample_id,
        'fastq_list': fastq_dict})
    return check_sum_sample_group
  except Exception as e:
    raise ValueError(
      f"Failed to get checksum for sample group fastq files, error: {e}")


def calculate_fastq_md5_checksum_func(**context):
  try:
    ti = context['ti']
    xcom_key_for_sample_group = \
      context['params'].\
      get("xcom_key_for_sample_group", "sample_group")
    xcom_task_for_sample_group = \
      context['params'].\
      get("xcom_task_for_sample_group")
    sample_group_id = \
      context['params'].\
      get("sample_group_id")
    xcom_key_for_checksum_sample_group = \
      context['params'].\
      get("xcom_key_for_checksum_sample_group", "checksum_sample_group")
    sample_group = \
      ti.xcom_pull(
        task_ids=xcom_task_for_sample_group,
        key=xcom_key_for_sample_group)
    if sample_group is None or \
       not isinstance(sample_group, list) or \
       len(sample_group) == 0:
      raise ValueError("sample_group is not a list")
    fastq_files_list = \
      get_sample_info_from_sample_group(
        worker_index=sample_group_id,
        sample_group=sample_group)
    sample_with_checksum_list = \
      get_checksum_for_sample_group_fastq_files(
        sample_group=fastq_files_list)
    ti.xcom_push(
      key=xcom_key_for_checksum_sample_group,
      value=sample_with_checksum_list)
  except Exception as e:
    log.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def get_jobs_per_worker(
      max_workers: int,
      total_jobs: int) \
        -> list:
  try:
    job_list = list()
    job_counter = 0
    if max_workers <= 0 or \
       total_jobs <= 0:
      raise ValueError("max_workers or total_jobs is not set")
    while job_counter <= total_jobs:
      for worker_index in range(1, max_workers + 1):
        job_counter += 1
        if job_counter <= total_jobs:
          job_list.append({
            'worker_index': worker_index,
            'jobs': job_counter})
    df = pd.DataFrame(job_list)
    df['jobs'] = \
      df['jobs'].astype('str')
    grp_df = \
      df.groupby('worker_index').\
      agg({'jobs': ','.join}, axis=1)
    grp_df = \
      grp_df.reset_index()
    grp_df['jobs'] = \
      grp_df['jobs'].str.split(',')
    return grp_df.to_dict(orient='records')
  except Exception as e:
    raise ValueError(
      f"Failed to divide jobs per worker, error: {e}")


def get_sample_groups_for_bcl_convert_output(
      samplesheet_file: str,
      max_samples: int = 20) \
        -> list:
  try:
    sa = SampleSheet(samplesheet_file)
    df = pd.DataFrame(sa._data)
    sample_id_list = \
      df['Sample_ID'].drop_duplicates().tolist()
    sample_groups = \
      get_jobs_per_worker(
        max_workers=max_samples,
        total_jobs=len(sample_id_list))
    sample_groups_list = list()
    for s in sample_groups:
      sample_ids = [
        sample_id_list[int(i)-1]
          for i in s.get('jobs')]
      sample_groups_list.append({
        'sample_ids': sample_ids,
        'worker_index': s.get('worker_index')})
    return sample_groups_list
  except Exception as e:
    raise ValueError(
      f"Failed to get sample groups for bcl convert output, error: {e}")


def get_sample_id_and_fastq_path_for_sample_groups(
      samplesheet_file: str,
      lane_id: int,
      bclconv_output_path: str,
      sample_group: list) \
        -> list:
  try:
    check_file_path(samplesheet_file)
    check_file_path(bclconv_output_path)
    ## TO Do the following
    #  * get sample_lists for sample_index from sample_group
    #  * get Sample_Project for each sample_ids (its fail safe)
    #  * get fastq_path for each sample_id assuming following path
    #      bclconv_output_path/Sample_Project/sample_id_S\d+_L\d+_[RIU][1-4]_001.fastq.gz
    sa = SampleSheet(samplesheet_file)
    samplesheet_df = pd.DataFrame(sa._data)
    samplesheet_df = \
      samplesheet_df[['Sample_ID', 'Sample_Project']].\
      drop_duplicates()
    formatted_sample_group = list()
    for entry in sample_group:
      worker_index = entry.get('worker_index')
      new_sample_groups_list = list()
      sample_groups_list = entry.get('sample_ids')
      for sample_id in sample_groups_list:
        fastq_file_regexp = \
          r'{0}_S\d+_L00{1}_[RIU][1-4]_001.fastq.gz'.\
          format(sample_id, lane_id)
        filt_df = \
          samplesheet_df[samplesheet_df['Sample_ID']==sample_id].fillna('')
        if len(filt_df.index)==0:
          raise ValueError(
            f"Sample_ID {sample_id} not found in samplesheet file {samplesheet_file}")
        sample_project = \
          filt_df['Sample_Project'].values.tolist()[0]
        base_fastq_path = \
          os.path.join(
            bclconv_output_path,
            sample_project)
        fastq_list_for_sample = list()
        for file_name in os.listdir(base_fastq_path):
          if re.search(fastq_file_regexp, file_name):
            fastq_list_for_sample.\
              append(
                os.path.join(
                  base_fastq_path,
                  file_name))
        new_sample_groups_list.\
          append({
            'sample_id': sample_id,
            'sample_project': sample_project,
            'fastq_list': fastq_list_for_sample})
      formatted_sample_group.append({
        'worker_index': worker_index,
        'sample_ids': new_sample_groups_list})
    return formatted_sample_group
  except Exception as e:
    raise ValueError(
      f"Failed to get sample fastq path for sample groups, error: {e}")


def sample_known_qc_factory_func(**context):
  try:
    ti = context['ti']
    samplesheet_file_suffix = \
      context['params'].\
      get("samplesheet_file_suffix", "Reports/SampleSheet.csv")
    xcom_key_for_bclconvert_output = \
      context['params'].\
      get("xcom_key_for_bclconvert_output", "bclconvert_output")
    xcom_task_for_bclconvert_output = \
      context['params'].\
      get("xcom_task_for_bclconvert_output", None)
    max_samples = \
      context['params'].\
      get("max_samples", 0)
    lane_index = \
      context['params'].\
      get("lane_index")
    xcom_key_for_sample_group = \
      context['params'].\
      get("xcom_key_for_sample_group", "sample_group")
    next_task_prefix = \
      context['params'].\
      get("next_task_prefix")
    bclconvert_output_dir = \
      ti.xcom_pull(
        task_ids=xcom_task_for_bclconvert_output,
        key=xcom_key_for_bclconvert_output)
    if bclconvert_output_dir is None:
      raise ValueError(
        f"Failed to get bcl convert output dir for task {xcom_task_for_bclconvert_output}")
    if max_samples == 0:
      raise ValueError("max_samples is not set")
    samplesheet_path = \
      os.path.join(
        bclconvert_output_dir,
        samplesheet_file_suffix)
    sample_groups_list = \
      get_sample_groups_for_bcl_convert_output(
        samplesheet_file=samplesheet_path,
        max_samples=max_samples)
    sample_group_with_fastq_path = \
      get_sample_id_and_fastq_path_for_sample_groups(
        samplesheet_file=samplesheet_path,
        lane_id=int(lane_index),
        bclconv_output_path=bclconvert_output_dir,
        sample_group=sample_groups_list)
    df = pd.DataFrame(sample_group_with_fastq_path)
    if 'worker_index' not in df.columns:
      raise ValueError("worker_index is not set")
    ti.xcom_push(
      key=xcom_key_for_sample_group,
      value=sample_group_with_fastq_path)
    sample_id_list = \
      df['worker_index'].\
      drop_duplicates().\
      values.tolist()
    task_list = list()
    for sample_id in sample_id_list:
      task_list.append(
        "{next_task_prefix}{sample_id}")
    return task_list
  except Exception as e:
    log.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def reset_single_cell_samplesheet(
      samplesheet_file: str,
      sampleid_col: str = 'Sample_ID',
      samplename_col: str = 'Sample_Name',
      filter_column_prefix: str = 'Original_',
      orig_sampleid_col : str = 'Original_Sample_ID',
      description_col: str = 'Description',
      orig_samplename_col: str = 'Original_Sample_Name',
      index_col: str = 'index',
      orig_index_col: str = 'Original_index',
      singlecell_tag: str = '10X') \
        -> None:
  try:
    check_file_path(samplesheet_file)
    samplesheet = \
      SampleSheet(samplesheet_file)
    samplesheet.\
      filter_sample_data(
        condition_key=description_col,
        condition_value=singlecell_tag,
        method='include')
    singlecell_df = \
      pd.DataFrame(samplesheet._data)
    samplesheet = \
      SampleSheet(samplesheet_file)
    samplesheet.\
      filter_sample_data(
        condition_key=description_col,
        condition_value=singlecell_tag,
        method='include')
    non_singlecell_df = \
      pd.DataFrame(samplesheet._data)
    if orig_sampleid_col not in singlecell_df.columns or \
       orig_samplename_col not in singlecell_df.columns or \
       orig_index_col not in singlecell_df.columns:
      raise ValueError(
        f"Original sample name, id or index columns not found in samplesheet file {samplesheet_file}")
    ## fix singlecell samplesheet
    singlecell_df[index_col] = \
      singlecell_df[orig_index_col]
    singlecell_df[sampleid_col] = \
      singlecell_df[orig_sampleid_col]
    singlecell_df[samplename_col] = \
      singlecell_df[orig_samplename_col]
    singlecell_df[description_col] = ''
    allowed_columns = [
      f for f in singlecell_df.columns
        if not f.startswith(filter_column_prefix)]
    if len(singlecell_df.index) > 0 and \
       len(allowed_columns) == 0:
      raise ValueError(
        f"No columns found after filtering samplesheet for {filter_column_prefix}")
    singlecell_df = \
      singlecell_df[allowed_columns]
    ## fix non-singlecell samplesheet
    allowed_columns = [
      f for f in non_singlecell_df.columns
        if not f.startswith(filter_column_prefix)]
    if len(non_singlecell_df.index) > 0 and \
        len(allowed_columns) == 0:
      raise ValueError(
        f"No columns found after filtering samplesheet for prefix {filter_column_prefix}")
    non_singlecell_df = \
      non_singlecell_df[allowed_columns]
    ## merge data and create modified samplesheet
    merged_data = list()
    singlecell_data = \
      singlecell_df.\
      to_dict(orient='records')
    if len(singlecell_data) > 0:
      merged_data.extend(singlecell_data)
    non_singlecell_data = \
      non_singlecell_df.\
      to_dict(orient='records')
    if len(non_singlecell_data) > 0:
      merged_data.extend(non_singlecell_data)
    merged_columns = \
      pd.DataFrame(merged_data).columns
    samplesheet = \
      SampleSheet(samplesheet_file)
    samplesheet._data_header = \
      merged_columns
    samplesheet._data = \
      merged_data
    ## move original samplesheet to backup
    backup_file = \
      samplesheet_file + '_original'
    copy_local_file(
      samplesheet_file,
      backup_file,
      force=True)
    os.remove(samplesheet_file)
    ## write modified samplesheet
    samplesheet.\
      print_sampleSheet(samplesheet_file)
  except Exception as e:
    raise ValueError(
      f"Failed to reset single cell samplesheet, error: {e}")


def merge_single_cell_fastq_files_func(**context):
  try:
    ti = context['ti']
    seqrun_igf_id = \
      context['params'].\
      get('seqrun_igf_id', None)
    singlecell_tag = \
      context['params'].\
      get('singlecell_tag', '10X')
    xcom_key_bclconvert_output = \
      context['params'].\
      get('xcom_key_bclconvert_output', 'bclconvert_output')
    xcom_task_bclconvert_output = \
      context['params'].\
      get('xcom_task_bclconvert_output', None)
    xcom_key_bclconvert_reports = \
      context['params'].\
      get('xcom_key_bclconvert_reports', 'bclconvert_reports')
    xcom_task_bclconvert_reports = \
      context['params'].\
      get('xcom_task_bclconvert_reports', None)
    samplesheet_file_suffix = \
      context['params'].\
      get('samplesheet_file_suffix', "SampleSheet.csv")
    bclconvert_output_path = \
      ti.xcom_pull(
        key=xcom_key_bclconvert_output,
        task_ids=xcom_task_bclconvert_output)
    bclconvert_reports_path = \
      ti.xcom_pull(
        key=xcom_key_bclconvert_reports,
        task_ids=xcom_task_bclconvert_reports)
    ## get original samplesheet
    samplesheet_path = \
      os.path.join(
        bclconvert_reports_path,
        samplesheet_file_suffix)
    check_file_path(samplesheet_path)
    formatted_samplesheets_list =\
      context['params'].\
      get('formatted_samplesheets')
    project_index = \
      context['params'].\
      get('project_index', 0)
    lane_index = \
      context['params'].\
      get('lane_index', 0)
    ig_index = \
      context['params'].\
      get('ig_index', 0)
    ## TO DO 1: merge fastq files for single cell samples
    if lane_index == 0:
      raise ValueError("lane_index is not set")
    if seqrun_igf_id is None:
      raise ValueError("seqrun_igf_id is not set")
    check_file_path(bclconvert_output_path)
    platform_name, _  = \
      get_platform_name_and_flowcell_id_for_seqrun(
        seqrun_igf_id=seqrun_igf_id,
        db_config_file=DATABASE_CONFIG_FILE)
    sc_data = \
      MergeSingleCellFastq(
        fastq_dir=bclconvert_output_path,
        samplesheet=samplesheet_path,
        platform_name=platform_name,
        use_bclconvert_settings=True,
        pseudo_lane_list=(str(lane_index),),
        singlecell_tag=singlecell_tag)
    sc_data.\
      merge_fastq_per_lane_per_sample()
    ## TO DO 2: reset samplesheet after merging for single cell samples
    reset_single_cell_samplesheet(
      samplesheet_file=samplesheet_path)
  except Exception as e:
    log.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def check_demult_stats_file_for_failed_samples(
      demult_stats_file: str,
      read_column: str = '# Reads',
      read_count_threshold: int = 500) -> bool:
  try:
    check_file_path(demult_stats_file)
    demult_stats_df = \
      pd.read_csv(demult_stats_file)
    filt_rows = \
      demult_stats_df[demult_stats_df[read_column] < read_count_threshold]
    if len(filt_rows.index) > 0:
      return False
    else:
      return True
  except Exception as e:
    raise ValueError("Failed to check demult stats file for failed samples")


def check_output_for_project_lane_index_group_func(**context):
  try:
    ti = context['ti']
    seqrun_igf_id = \
      context['params'].\
      get('seqrun_igf_id', None)
    demult_stats_file_name = \
      context['params'].\
      get('demult_stats_file_name', 'Demultiplex_Stats.csv')
    xcom_key_bclconvert_reports = \
      context['params'].\
      get('xcom_key_bclconvert_reports', 'bclconvert_reports')
    xcom_task_bclconvert_reports = \
      context['params'].\
      get('xcom_task_bclconvert_reports', None)
    read_count_threshold = \
      context['params'].\
      get('read_count_threshold', 500)
    if seqrun_igf_id is None:
      raise ValueError("seqrun_igf_id is not set")
    bclconvert_reports_path = \
      ti.xcom_pull(
        key=xcom_key_bclconvert_reports,
        task_ids=xcom_task_bclconvert_reports)
    demult_stats_file_path = \
      os.path.join(
        bclconvert_reports_path,
        demult_stats_file_name)
    check_file_path(demult_stats_file_path)
    check_status = \
      check_demult_stats_file_for_failed_samples(
        demult_stats_file=demult_stats_file_path,
        read_count_threshold=read_count_threshold)
    if not check_status:
      raise ValueError(
        f"Run {seqrun_igf_id} failing read count validation")
  except Exception as e:
    log.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def generate_bclconvert_report(
      seqrun_path: str,
      image_path: str,
      report_template: str,
      bclconvert_report_library_path: str,
      bclconvert_reports_path: str,
      dry_run: bool = False) \
        -> str:
  try:
    check_file_path(seqrun_path)
    check_file_path(image_path)
    check_file_path(report_template)
    check_file_path(bclconvert_reports_path)
    check_file_path(bclconvert_report_library_path)
    temp_run_dir = get_temp_dir()
    interop_dir = os.path.join(seqrun_path, 'InterOp')
    runinfo_xml = os.path.join(seqrun_path, 'RunInfo.xml')
    check_file_path(runinfo_xml)
    index_metric_bin = \
      os.path.join(
        bclconvert_reports_path,
        'IndexMetricsOut.bin')
    check_file_path(interop_dir)
    check_file_path(index_metric_bin)
    copy_local_file(
      interop_dir,
      os.path.join(temp_run_dir, 'InterOp'))
    copy_local_file(
      runinfo_xml,
      os.path.join(temp_run_dir, 'RunInfo.xml'))
    copy_local_file(
      index_metric_bin,
      os.path.join(
        temp_run_dir,
        'InterOp',
        'IndexMetricsOut.bin'),
      force=True)
    input_params = {
      'DATE_TAG': get_date_stamp(),
      'SEQRUN_IGF_ID': os.path.basename(seqrun_path.strip('/')),
      'REPORTS_DIR': bclconvert_reports_path,
      'RUN_DIR': temp_run_dir}
    container_bind_dir_list = [
      temp_run_dir,
      bclconvert_reports_path,
      bclconvert_report_library_path]
    temp_dir = get_temp_dir()
    nb = \
      Notebook_runner(
        template_ipynb_path=report_template,
        output_dir=temp_dir,
        input_param_map=input_params,
        container_paths=container_bind_dir_list,
        kernel='python3',
        use_ephemeral_space=True,
        singularity_options=['--no-home','-C', "--env", f"PYTHONPATH={bclconvert_report_library_path}"],
        allow_errors=False,
        singularity_image_path=image_path,
        dry_run=dry_run)
    output_notebook_path, _ = \
      nb.execute_notebook_in_singularity()
    return output_notebook_path
  except Exception as e:
    raise ValueError(
      f"Failed to generate bclconvert report, error: {e}")


def bclconvert_report_func(**context):
  try:
    ti = context['ti']
    seqrun_igf_id = \
      context['params'].\
      get('seqrun_igf_id', None)
    xcom_key_for_reports = \
      context['params'].\
      get('xcom_key_for_reports', 'bclconvert_reports')
    xcom_task_for_reports = \
      context['params'].\
      get('xcom_task_for_reports', None)
    bclconvert_reports_path = \
      ti.xcom_pull(
        key=xcom_key_for_reports,
        task_ids=xcom_task_for_reports)
    # dag_run = context.get('dag_run')
    # seqrun_path = None
    # if dag_run is not None and \
    #    dag_run.conf is not None and \
    #    dag_run.conf.get('seqrun_id') is not None:
    #   seqrun_id = \
    #     dag_run.conf.get('seqrun_id')
    #   seqrun_path = \
    #     os.path.join(HPC_SEQRUN_BASE_PATH, seqrun_id)
    # else:
    #   raise IOError("Failed to get seqrun_id from dag_run")
    if seqrun_igf_id is None:
      raise ValueError("seqrun_igf_id is not set")
    seqrun_path = \
      os.path.join(
        HPC_SEQRUN_BASE_PATH,
        seqrun_igf_id)
    report_file = \
      generate_bclconvert_report(
        seqrun_path=seqrun_path,
        image_path=INTEROP_NOTEBOOK_IMAGE,
        report_template=BCLCONVERT_REPORT_TEMPLATE,
        bclconvert_report_library_path=BCLCONVERT_REPORT_LIBRARY,
        bclconvert_reports_path=bclconvert_reports_path)
    copy_local_file(
      report_file,
      os.path.join(
        bclconvert_reports_path,
        os.path.basename(report_file)))
  except Exception as e:
    log.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def bclconvert_singularity_wrapper(
      image_path: str,
      input_dir: str,
      output_dir: str,
      samplesheet_file: str,
      bcl_num_conversion_threads: int = 1,
      bcl_num_compression_threads: int = 1,
      bcl_num_decompression_threads: int = 1,
      bcl_num_parallel_tiles: int = 1,
      lane_id : int = 0,
      tile_id_list: tuple = (),
      first_tile_only: bool = False,
      dry_run: bool = False) \
      -> str:
  try:
    check_file_path(image_path)
    check_file_path(input_dir)
    if os.path.exists(output_dir):
      raise ValueError(f"Output directory {output_dir} already exists")
    check_file_path(samplesheet_file)
    temp_dir = get_temp_dir(use_ephemeral_space=True)
    bclconvert_cmd = [
      "bcl-convert",
      "--bcl-input-directory", input_dir,
      "--output-directory", output_dir,
      "--sample-sheet", samplesheet_file,
      "--bcl-num-conversion-threads", str(bcl_num_conversion_threads),
      "--bcl-num-compression-threads", str(bcl_num_compression_threads),
      "--bcl-num-decompression-threads", str(bcl_num_decompression_threads),
      "--bcl-num-parallel-tiles", str(bcl_num_parallel_tiles),
      "--bcl-sampleproject-subdirectories", "true",
      "--strict-mode", "true"]
    if first_tile_only:
      bclconvert_cmd.\
        extend(["--first-tile-only", "true"])
    if lane_id > 0:
      bclconvert_cmd.\
        extend(["--bcl-only-lane", str(lane_id)])
    if len(tile_id_list) > 0:
      bclconvert_cmd.\
        extend(["--tiles", ",".join(tile_id_list)])
    bclconvert_cmd = \
      ' '.join(bclconvert_cmd)
    bind_paths = [
      f'{temp_dir}:/var/log',
      os.path.dirname(samplesheet_file),
      input_dir,
      os.path.dirname(output_dir)]
    cmd = execute_singuarity_cmd(
      image_path=image_path,
      command_string=bclconvert_cmd,
      bind_dir_list=bind_paths,
      dry_run=dry_run)
    return cmd
  except:
    raise

def run_bclconvert_func(**context):
  try:
    ti = context['ti']
    seqrun_igf_id = \
      context['params'].\
      get('seqrun_igf_id')
    formatted_samplesheets_list =\
      context['params'].\
      get('formatted_samplesheets')
    # xcom_key = \
    #   context['params'].\
    #   get('xcom_key', 'formatted_samplesheets')
    # xcom_task = \
    #   context['params'].\
    #   get('xcom_task', 'format_and_split_samplesheet')
    project_index_column = \
      context['params'].\
      get('project_index_column', 'project_index')
    project_index = \
      context['params'].\
      get('project_index', 0)
    project_column = \
      context['params'].\
      get('project_column', 'project')
    lane_index_column = \
      context['params'].\
      get('lane_index_column', 'lane_index')
    lane_column = \
      context['params'].\
      get('lane_column', 'lane')
    lane_index = \
      context['params'].\
      get('lane_index', 0)
    ig_index_column = \
      context['params'].\
      get('ig_index_column', 'index_group_index')
    index_group_column = \
      context['params'].\
      get('index_group_column', 'index_group')
    ig_index = \
      context['params'].\
      get('ig_index', 0)
    samplesheet_column = \
      context['params'].\
      get('samplesheet_column', 'samplesheet_file')
    xcom_key_for_reports = \
      context['params'].\
      get('xcom_key_for_reports', 'bclconvert_reports')
    xcom_key_for_output = \
      context['params'].\
      get('xcom_key_for_output', 'bclconvert_output')
    bcl_num_conversion_threads = \
      context['params'].\
      get('bcl_num_conversion_threads', '1')
    bcl_num_compression_threads = \
      context['params'].\
      get('bcl_num_compression_threads', '1')
    bcl_num_decompression_threads = \
      context['params'].\
      get('bcl_num_decompression_threads', '1')
    bcl_num_parallel_tiles = \
      context['params'].\
      get('bcl_num_parallel_tiles', '1')
    # dag_run = context.get('dag_run')
    seqrun_path = ''
    # if dag_run is not None and \
    #    dag_run.conf is not None and \
    #    dag_run.conf.get('seqrun_id') is not None:
    #   seqrun_id = \
    #     dag_run.conf.get('seqrun_id')
    #   seqrun_path = \
    #     os.path.join(HPC_SEQRUN_BASE_PATH, seqrun_id)
    # else:
    #   raise IOError(
    #     "Failed to get seqrun_id from dag_run")
    if seqrun_igf_id is None:
      raise IOError(
        "Failed to get seqrun_igf_id")
    seqrun_path = \
      os.path.join(
        HPC_SEQRUN_BASE_PATH,
        seqrun_igf_id)
    if project_index == 0 or \
       lane_index == 0 or \
       ig_index == 0:
      raise ValueError(
        'project_index, lane_index or ig_index is not set')
    # if xcom_key is None or \
    #    xcom_task is None:
    #   raise ValueError('xcom_key or xcom_task is not set')
    # formatted_samplesheets_list = \
    #   ti.xcom_pull(task_ids=xcom_task, key=xcom_key)
    df = pd.DataFrame(formatted_samplesheets_list)
    if project_index_column not in df.columns or \
        lane_index_column not in df.columns or \
        lane_column not in df.columns or \
        ig_index_column not in df.columns or \
        samplesheet_column not in df.columns:
      raise KeyError(""""
        project_index_column, lane_index_column, lane_column,
        ig_index_column or samplesheet_column is not found""")
    ig_df = \
      df[
        (df[project_index_column]==project_index) &
        (df[lane_index_column]==lane_index) &
        (df[ig_index_column]==ig_index)]
    if len(ig_df.index) == 0:
      raise ValueError(
        f"No samplesheet found for project {project_index}, lane {lane_index}, ig {ig_index}")
    samplesheet_file = \
      ig_df[samplesheet_column].values.tolist()[0]
    output_dir = \
      ig_df['output_dir'].values.tolist()[0]
    project_id = \
      ig_df[project_column].values.tolist()[0]
    lane_id = \
      ig_df[lane_column].values.tolist()[0]
    ig_id = \
      ig_df[index_group_column].values.tolist()[0]
    output_temp_dir = \
      get_temp_dir(use_ephemeral_space=True)
    demult_dir = \
      os.path.join(
        output_temp_dir,
        f'{project_id}_{lane_id}_{ig_id}')
    cmd = \
      bclconvert_singularity_wrapper(
        image_path=BCLCONVERT_IMAGE,
        input_dir=seqrun_path,
        output_dir=demult_dir,
        samplesheet_file=samplesheet_file,
        bcl_num_conversion_threads=int(bcl_num_conversion_threads),
        bcl_num_compression_threads=int(bcl_num_compression_threads),
        bcl_num_decompression_threads=int(bcl_num_decompression_threads),
        bcl_num_parallel_tiles=int(bcl_num_parallel_tiles),
        lane_id=int(lane_id))
    check_file_path(demult_dir)    # check if the output dir exists
    check_file_path(
      os.path.join(
        demult_dir,
        'Reports',
        'Demultiplex_Stats.csv'))  # check if the demultiplex stats file exists
    bclconvert_output_dir = \
      os.path.join(
        output_dir,
        '{0}_{1}_{2}'.format(
          project_id,
          lane_id,
          ig_id))                  # output dir for bclconvert
    reports_dir = \
      os.path.join(
        bclconvert_output_dir,
        'Reports')                 # output dir for bclconvert reports
    copy_local_file(
      demult_dir,
      bclconvert_output_dir)       # copy the output dir to the output dir
    check_file_path(reports_dir)   # check if the reports dir exists after copy
    ti.xcom_push(
      key=xcom_key_for_reports,
      value=reports_dir)
    ti.xcom_push(
      key=xcom_key_for_output,
      value=bclconvert_output_dir)
    message = \
      f'Finished demultiplexing project {project_id}, lane {lane_id}, ig {ig_id} - cmd: {cmd}'
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='pass')
  except Exception as e:
    log.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def trigger_ig_jobs(**context):
  try:
    ti = context['ti']
    sample_groups = \
      context['params'].\
      get('sample_groups')
    # xcom_key = \
    #   context['params'].\
    #   get('xcom_key', 'formatted_samplesheets')
    # xcom_task = \
    #   context['params'].\
    #   get('xcom_task', 'format_and_split_samplesheet')
    # project_index_column = \
    #   context['params'].\
    #   get('project_index_column', 'project_index')
    project_index = \
      context['params'].\
      get('project_index', 0)
    # lane_index_column = \
    #   context['params'].get('lane_index_column', 'lane_index')
    lane_index = \
      context['params'].\
      get('lane_index', 0)
    ig_task_prefix = \
      context['params'].\
      get('ig_task_prefix')
    max_index_groups = \
      context['params'].\
      get('max_index_groups')
    # ig_index_column = \
    #   context['params'].get('ig_index_column', 'index_group_index')
    # formatted_samplesheets_list = \
    #   ti.xcom_pull(task_ids=xcom_task, key=xcom_key)
    # if len(formatted_samplesheets_list) == 0:
    #   raise ValueError(
    #           "No samplesheet found for seqrun {0}".\
    #           format(context['dag_run'].conf.get('seqrun_id')))
    # df = pd.DataFrame(formatted_samplesheets_list)
    if project_index == 0 :
      raise ValueError("Invalid projext index 0")
    # if project_index_column not in df.columns:
    #   raise KeyError("Column {0} not found in samplesheet".\
    #                  format(project_index_column))
    if lane_index == 0 :
      raise ValueError("Invalid lane index 0")
    # if lane_index_column not in df.columns:
    #   raise KeyError("Column {0} not found in samplesheet".\
    #                   format(lane_index_column))
    # if ig_index_column not in df.columns:
    #   raise KeyError("Column {0} not found in samplesheet".\
    #                   format(ig_index_column))
    # df[project_index_column] = df[project_index_column].astype(int)
    # df[lane_index_column] = df[lane_index_column].astype(int)
    # df[ig_index_column] = df[ig_index_column].astype(int)
    # project_df = df[df[project_index_column] == int(project_index)]
    # lane_df = project_df[project_df[lane_index_column] == int(lane_index)]
    # if len(lane_df.index) == 0:
    #   raise ValueError("No samplesheet found for project {0}, lane {1}".\
    #                    format(project_index, lane_index))
    # ig_counts = \
    #   lane_df[ig_index_column].\
    #   drop_duplicates().\
    #   values.\
    #   tolist()
    ig_counts = \
      sample_groups.\
      get(project_index).\
      get(lane_index).\
      keys()
    if len(ig_counts) == 0:
      raise ValueError(
        f"No index group found for project {project_index}, lane {lane_index}")
    if len(ig_counts) > int(max_index_groups):
      raise ValueError(
        f"Too many index groups found for project {project_index}, lane {lane_index}")
    task_list = [
      f'{ig_task_prefix}{ig}'
         for ig in ig_counts]
    return task_list
  except Exception as e:
    log.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def trigger_lane_jobs(**context):
  try:
    ti = context['ti']
    sample_groups = \
      context['params'].\
      get('sample_groups')
    # xcom_key = \
    #   context['params'].\
    #   get('xcom_key', 'formatted_samplesheets')
    # xcom_task = \
    #   context['params'].\
    #   get('xcom_task', 'format_and_split_samplesheet')
    # project_index_column = \
    #   context['params'].\
    #   get('project_index_column', 'project_index')
    project_index = \
      context['params'].\
      get('project_index', 0)
    # lane_index_column = \
    #   context['params'].\
    #   get('lane_index_column', 'lane_index')
    lane_task_prefix = \
      context['params'].\
      get('lane_task_prefix')
    max_lanes = \
      context['params'].\
      get('max_lanes', 0)
    # formatted_samplesheets_list = \
    #   ti.xcom_pull(
    #     task_ids=xcom_task,
    #     key=xcom_key)
    # if len(formatted_samplesheets_list) == 0:
    #   seqrun_id = \
    #     context['dag_run'].conf.get('seqrun_id')
    #   raise ValueError(
    #     f"No samplesheet found for seqrun {seqrun_id}")
    # df = \
    #   pd.DataFrame(formatted_samplesheets_list)
    if project_index == 0 :
      raise ValueError("Invalid projext index 0")
    # if project_index_column not in df.columns:
    #   raise KeyError(
    #     f"Column {project_index_column} not found in samplesheet")
    # if lane_index_column not in df.columns:
    #   raise KeyError(
    #     f"Column {lane_index_column} not found in samplesheet")
    # df[project_index_column] = \
    #   df[project_index_column].\
    #   astype(int)
    # project_df = \
    #   df[df[project_index_column] == int(project_index)]
    # lane_counts = \
    #   project_df[lane_index_column].\
    #   drop_duplicates().\
    #   values.tolist()
    lane_counts = \
      sample_groups.\
      get(project_index).keys()
    if len(lane_counts) == 0:
      raise ValueError(
        f"No lane found for project {project_index}")
    if len(lane_counts) > int(max_lanes):
      raise ValueError(
        f"Too many lanes {lane_counts} found for project {project_index}")
    task_list = [
      f'{lane_task_prefix}{lane_count}'
        for lane_count in lane_counts]
    return task_list
  except Exception as e:
    log.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def setup_globus_transfer_for_project_func(**context):
  """
  Create a temp dir in Ephemeral space and add to xcom
  """
  try:
    ti = context['ti']
    seqrun_igf_id = \
      context['params'].\
      get('seqrun_igf_id')
    globus_dir_xcom_key = \
      context['params'].\
      get('globus_dir_xcom_key', 'globus_root_dir')
    formatted_samplesheets_list = \
      context['params'].\
      get('formatted_samplesheets')
    # project_data_xcom_key = \
    #   context['params'].\
    #   get('project_data_xcom_key', 'formatted_samplesheets')
    # project_data_xcom_task = \
    #   context['params'].\
    #   get('project_data_xcom_task', 'format_and_split_samplesheet')
    project_index_column = \
      context['params'].\
      get('project_index_column', 'project_index')
    project_index = \
      context['params'].\
      get('project_index')
    project_column = \
      context['params'].\
      get('project_column', 'project')
    ## get serun id
    # dag_run = context.get('dag_run')
    # seqrun_id = ''
    # if dag_run is not None and \
    #    dag_run.conf is not None and \
    #    dag_run.conf.get('seqrun_id') is not None:
    #   seqrun_id = \
    #     dag_run.conf.get('seqrun_id')
    # else:
    #   raise IOError("Failed to get seqrun_id from dag_run")
    ## get flowcell id from db
    _, flowcell_id = \
      get_flatform_name_and_flowcell_id_for_seqrun(
        seqrun_igf_id=seqrun_igf_id,
        db_config_file=DATABASE_CONFIG_FILE)
    ## get seqrun date from seqrun id
    seqrun_date = \
      get_seqrun_date_from_igf_id(
        seqrun_igf_id=seqrun_igf_id)
    ## fetch project name
    # formatted_samplesheets_list = \
    #   ti.xcom_pull(
    #     task_ids=project_data_xcom_task,
    #     key=project_data_xcom_key)
    df = \
      pd.DataFrame(
        formatted_samplesheets_list)
    if project_index_column not in df.columns:
      raise KeyError(
        f"{project_index_column} column not found")
    df[project_index_column] = \
      df[project_index_column].astype(int)
    project_df = \
      df[df[project_index_column]==int(project_index)]
    project_name = \
      project_df[project_column].values.tolist()[0]
    ## temp_dir / project name _ run_date _ flowcell_id
    globus_root_dir = \
      get_temp_dir(use_ephemeral_space=True)
    globus_project_dir = \
      os.path.join(
        globus_root_dir,
        f"{project_name}_{flowcell_id}_{seqrun_date}")
    ti.xcom_push(
      key=globus_dir_xcom_key,
      value=globus_project_dir)
  except Exception as e:
    log.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def _get_project_user_list(
      db_config_file: str,
      project_name: str) -> \
      Tuple[list, dict, bool]:
  try:
    dbparams = \
      read_dbconf_json(db_config_file)
    pa = ProjectAdaptor(**dbparams)
    pa.start_session()
    user_info = \
      pa.get_project_user_info(
        project_igf_id=project_name)                             # fetch user info from db
    pa.close_session()
    user_info = \
      user_info.to_dict(orient='records')
    if len(user_info) == 0:
      raise ValueError(
        f"No user found for project {project_name}")
    user_list = list()
    user_passwd_dict = dict()
    hpc_user = True
    for user in user_info:
      username = user['username']
      user_list.append(username)
      if 'ht_password' in user.keys():
        ht_passwd = user['ht_password']
        user_passwd_dict.\
          update({username: ht_passwd})
      if 'category' in user.keys() and \
           'data_authority' in user.keys() and \
           user['category'] == 'NON_HPC_USER' and \
           user['data_authority']=='T':
          hpc_user = False
    return user_list, user_passwd_dict, hpc_user
  except Exception as e:
    raise ValueError(
      f"Failed to get user list for project {project_name}, error: {e}")


def _get_project_sample_count(
      db_config_file: str,
      project_name: str,
      only_active: bool = True) -> int:
  try:
    dbparams = \
      read_dbconf_json(db_config_file)
    pa = ProjectAdaptor(**dbparams)
    pa.start_session()
    sample_counts = \
      pa.count_project_samples(\
        project_igf_id=project_name,
        only_active=only_active)
    pa.close_session()
    return sample_counts
  except Exception as e:
    raise ValueError(
      f"Failed to get project samples, error: {e}")


def _configure_qc_pages_for_ftp(
      template_dir: str,
      project_name: str,
      db_config_file: str,
      remote_project_base_path: str,
      output_path: str = '',
      htaccess_template: str = 'ht_access/htaccess',
      htpasswd_template: str = 'ht_access/htpasswd',
      project_template: str = 'project_info/index.html',
      status_template: str = 'project_info/status.html',
      analysis_template: str = 'project_info/analysis.html',
      analysis_viewer_template: str = 'project_info/analysis_viewer.html',
      seqruninfofile: str = 'seqruninfofile.json',
      samplereadcountfile: str = 'samplereadcountfile.json',
      samplereadcountcsvfile: str = 'samplereadcountfile.csv',
      status_data_json: str = 'status_data.json',
      analysis_data_json: str = 'analysis_data.json',
      analysis_data_csv: str = 'analysis_data.csv',
      analysis_chart_data_csv: str = 'analysis_chart_data.csv',
      analysis_chart_data_json: str = 'analysis_chart_data.json',
      analysis_view_js: str = 'viewer.js',
      project_image_height: int = 700,
      project_sample_count_threshold: int = 75
      ) -> dict:
  try:
    ## get template paths
    htaccess_template_path = \
      os.path.join(
        template_dir,
        htaccess_template)
    check_file_path(htaccess_template_path)
    htpasswd_template_path = \
      os.path.join(
        template_dir,
        htpasswd_template)
    check_file_path(htpasswd_template_path)
    project_template_path = \
      os.path.join(
        template_dir,
        project_template)
    check_file_path(project_template_path)
    status_template_path = \
      os.path.join(
        template_dir,
        status_template)
    check_file_path(status_template_path)
    analysis_template_path = \
      os.path.join(
        template_dir,
        analysis_template)
    check_file_path(analysis_template_path)
    analysis_viewer_template = \
      os.path.join(
        template_dir,
        analysis_viewer_template)
    check_file_path(analysis_viewer_template)
    ## get projects user list and sample count from db
    user_list, user_passwd_dict, hpc_user = \
      _get_project_user_list(
        db_config_file=db_config_file,
        project_name=project_name)
    sample_counts = \
      _get_project_sample_count(
        db_config_file=db_config_file,
        project_name=project_name,
        only_active=True)
    ## get image height for project page
    image_height = \
      _calculate_image_height_for_project_page(
        sample_count=sample_counts,
        height=project_image_height,
        threshold=project_sample_count_threshold)
    ## create output in temp dir
    if output_path != '':
      check_file_path(output_path)
      temp_work_dir = output_path
    else:
      temp_work_dir = \
        get_temp_dir(use_ephemeral_space=True)
    ## htaccess file
    htaccess_output = \
      os.path.join(
        temp_work_dir,
        ".{0}".format(os.path.basename(htaccess_template_path)))
    _create_output_from_jinja_template(
      template_file=htaccess_template_path,
      output_file=htaccess_output,
      autoescape_list=['html', 'xml'],
      data=dict(
        remote_project_dir=remote_project_base_path,
        project_tag=project_name,
        hpcUser=hpc_user,
        htpasswd_filename='.{0}'.format(os.path.basename(htpasswd_template)),
        customerUsernameList=' '.join(user_list)))
    ## htpasswd file
    htpasswd_output = \
      os.path.join(
        temp_work_dir,
        ".{0}".format(os.path.basename(htpasswd_template_path)))
    _create_output_from_jinja_template(
      template_file=htpasswd_template_path,
      output_file=htpasswd_output,
      autoescape_list=['html', 'xml'],
      data=dict(userDict=user_passwd_dict))
    ## project page
    project_output = \
      os.path.join(
        temp_work_dir,
        os.path.basename(project_template_path))
    _create_output_from_jinja_template(
      template_file=project_template_path,
      output_file=project_output,
      autoescape_list=['txt', 'xml'],
      data=dict(
        ProjectName=project_name,
        seqrunInfoFile=seqruninfofile,
        sampleReadCountFile=samplereadcountfile,
        sampleReadCountCsvFile=samplereadcountcsvfile,
        ImageHeight=image_height))
    ## status page
    status_output = \
      os.path.join(
        temp_work_dir,
        os.path.basename(status_template_path))
    _create_output_from_jinja_template(
      template_file=status_template_path,
      output_file=status_output,
      autoescape_list=['txt', 'xml'],
      data=dict(
        ProjectName=project_name,
        status_data_json=status_data_json))
    ## analysis page
    analysis_output = \
      os.path.join(
        temp_work_dir,
        os.path.basename(analysis_template_path))
    _create_output_from_jinja_template(
      template_file=analysis_template_path,
      output_file=analysis_output,
      autoescape_list=['txt', 'xml'],
      data=dict(
        ProjectName=project_name,
        analysisInfoFile=analysis_data_json,
        analysisInfoCsvFile=analysis_data_csv,
        analysisCsvDataFile=analysis_chart_data_csv,
        analysisPlotFile=analysis_chart_data_json))
    ## analysis viewer page
    analysis_viewer_output = \
      os.path.join(
        temp_work_dir,
        os.path.basename(analysis_viewer_template))
    _create_output_from_jinja_template(
      template_file=analysis_viewer_template,
      output_file=analysis_viewer_output,
      autoescape_list=['txt', 'xml'],
      data=dict(
        ProjectName=project_name,
        analysisJsFile=analysis_view_js))
    ## get remote page paths
    remote_project_dir = \
      os.path.join(
        remote_project_base_path,
        project_name)
    remote_htaccess_file = \
      os.path.join(
        remote_project_dir,
        os.path.basename(htaccess_output))
    remote_htpasswd_file = \
      os.path.join(
        remote_project_dir,
        os.path.basename(htpasswd_output))
    remote_project_output_file = \
      os.path.join(
        remote_project_dir,
        os.path.basename(project_output))
    remote_status_output_file = \
      os.path.join(
        remote_project_dir,
        os.path.basename(status_output))
    remote_analysis_output_file = \
      os.path.join(
        remote_project_dir,
        os.path.basename(analysis_output))
    remote_analysis_viewer_output_file = \
      os.path.join(
        remote_project_dir,
        os.path.basename(analysis_viewer_output))
    output_file_dict = {
      htaccess_output: remote_htaccess_file,
      htpasswd_output: remote_htpasswd_file,
      project_output: remote_project_output_file,
      status_output: remote_status_output_file,
      analysis_output: remote_analysis_output_file,
      analysis_viewer_output: remote_analysis_viewer_output_file}
    return output_file_dict
  except Exception as e:
    raise ValueError(
      f"Failed to configure qc pages for ftp, error: {e}")


def setup_qc_page_for_project_func(**context):
  try:
    ti = context.get('ti')
    formatted_samplesheets_list = \
      context['params'].\
      get('formatted_samplesheets')
    # project_data_xcom_key = \
    #   context['params'].\
    #   get('project_data_xcom_key', 'formatted_samplesheets')
    # project_data_xcom_task = \
    #   context['params'].\
    #   get('project_data_xcom_task', 'format_and_split_samplesheet')
    project_index_column = \
      context['params'].\
      get('project_index_column', 'project_index')
    project_index = \
      context['params'].\
      get('project_index')
    project_column = \
      context['params'].\
      get('project_column', 'project')
    ## fetch project name
    # formatted_samplesheets_list = \
    #   ti.xcom_pull(
    #     task_ids=project_data_xcom_task,
    #     key=project_data_xcom_key)
    df = \
      pd.DataFrame(
        formatted_samplesheets_list)
    if project_index_column not in df.columns:
      raise KeyError(f"{project_index_column} column not found")
    df[project_index_column] = \
      df[project_index_column].astype(int)
    project_df = \
      df[df[project_index_column]==int(project_index)]
    project_name = \
      project_df[project_column].values.tolist()[0]
    ## dump qc pages to temp dir
    output_file_dict = \
      _configure_qc_pages_for_ftp(
      template_dir=QC_PAGE_TEMPLATE_DIR,
      project_name=project_name,
      remote_project_base_path=FTP_PROJECT_PATH,
      db_config_file=DATABASE_CONFIG_FILE)
    ## upload qc pages to remote server
    for local_file, remote_file in output_file_dict.items():
      os.chmod(local_file, mode=0o774)
      copy_remote_file(
      source_path=local_file,
      destination_path=remote_file,
      destination_address=f"{FTP_USERNAME}@{FTP_HOSTNAME}",
      ssh_key_file=HPC_SSH_KEY_FILE,
      force_update=True)
    return True
  except Exception as e:
    log.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def _create_output_from_jinja_template(
      template_file: str,
      output_file: str,
      autoescape_list: list,
      data: dict) -> None:
  try:
    template_env = \
      Environment(\
        loader=FileSystemLoader(
          searchpath=os.path.dirname(template_file)),
          autoescape=select_autoescape(autoescape_list))
    template = \
      template_env.\
        get_template(
          os.path.basename(template_file))
    template.\
      stream(**data).\
      dump(output_file)
    check_file_path(output_file)
  except Exception as e:
    raise ValueError(
      f"Failed to create output file usinh jinja, error {e}")


def _calculate_image_height_for_project_page(
      sample_count: int,
      height: int = 700,
      threshold: int = 75) -> int:
    '''
    An internal static method for calculating image height based on the number
    of samples registered for any projects

    :param sample_count: Sample count for a given project
    :param height: Height of the image of display page, default 700
    :param threshold: Sample count threshold, default 75
    :returns: Revised image height
    '''
    try:
      if sample_count <= threshold:                                             # low sample count
        return height
      else:
        if (sample_count / threshold) <= 2:                                     # high sample count
          return height * 2
        else:                                                                   # very high sample count
          return int(height * (2 + math.log(sample_count / threshold)))
    except Exception as e:
      raise ValueError(e)


def format_and_split_samplesheet_func(**context):
  try:
    # ti = context['ti']
    # seqrun_igf_id = \
    #   context['params'].\
    #   get('seqrun_igf_id')
    # formatted_samplesheets = \
    #   context['params'].\
    #   get('formatted_samplesheets')
    sample_groups = \
      context['params'].\
      get('sample_groups')
    # xcom_key = \
    #   context['params'].\
    #   get('xcom_key', 'formatted_samplesheets')
    max_projects = \
      context['params'].\
      get('max_projects', 0)
    project_task_prefix = \
      context['params'].\
      get('project_task_prefix', 'setup_qc_page_for_project_')
    # samplesheet_xcom_key = \
    #   context['params'].\
    #   get('samplesheet_xcom_key', 'samplesheet_data')
    # samplesheet_xcom_task = \
    #   context['params'].\
    #   get('samplesheet_xcom_task', 'fetch_samplesheet_for_run')
    # dag_run = context.get('dag_run')
    task_list = ['mark_run_finished',]
    # if dag_run is not None and \
    #    dag_run.conf is not None and \
    #    dag_run.conf.get('seqrun_id') is not None:
    #   seqrun_id = \
    #     dag_run.conf.get('seqrun_id')
    #   seqrun_path = \
    #     os.path.join(HPC_SEQRUN_BASE_PATH, seqrun_id)
    # else:
    #   raise("No seqrun_id found in dag_run conf")
    ## fetch samplesheet from previous task
    # samplesheet_file = \
    #   ti.xcom_pull(
    #     task_ids=samplesheet_xcom_task,
    #     key=samplesheet_xcom_key)
    # check_file_path(samplesheet_file)
    # runinfo_xml_file = \
    #   os.path.join(
    #     seqrun_path,
    #     'RunInfo.xml')
    # check_file_path(samplesheet_file)
    # samplesheet_dir = \
    #   get_temp_dir(use_ephemeral_space=True)
    ## get formatted samplesheets
    ## output:
    ## [{
	  ##   'project': 'project_name',
	  ##   'project_index': 1,
	  ##   'lane': 1,
	  ##   'lane_index': 1,
	  ##   'bases_mask': 'Y28;I10;I10;Y90',
	  ##   'index_group': '20_10X',
	  ##   'index_group_index': 1,
	  ##   'samplesheet_file': '/tmp/SampleSheet_project_name_1_20_NA.csv',
	  ##   'output_dir': '/tmp/dir'
    ## }]
    # formatted_samplesheets_list = \
    #   _get_formatted_samplesheets(
    #     samplesheet_file=samplesheet_file,
    #     runinfo_xml_file=runinfo_xml_file,
    #     samplesheet_output_dir=samplesheet_dir,
    #     singlecell_barcode_json=SINGLECELL_BARCODE_JSON,
    #     singlecell_dual_barcode_json=SINGLECELL_DUAL_BARCODE_JSON)
    ## save formatted samplesheet data to xcom
    # ti.xcom_push(
    #   key=xcom_key,
    #   value=formatted_samplesheets_list)
    # project_indices = \
    #   pd.DataFrame(formatted_samplesheets_list)['project_index'].\
    #   drop_duplicates().values.tolist()
    # if len(project_indices) > max_projects:
    #   raise ValueError(
    #     f"Too many projects {project_indices}. Increase MAX_PROJECTS param from {max_projects}")
    if len(sample_groups.keys()) > max_projects:
      raise ValueError(
        f"Too many projects {sample_groups.keys()}. Increase MAX_PROJECTS param from {max_projects}")
    ## generate task list
    # task_list = [
    #   f'{project_task_prefix}{project_index}'
    #     for project_index in project_indices]
    task_list = [
      f'{project_task_prefix}{project_index}'
        for project_index in sample_groups.keys()]
    if len(task_list) == 0:
      log.warning(
        f"No project indices found in sample_groups {sample_groups}")
      task_list = ['mark_run_finished']
    return task_list
  except Exception as e:
    log.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def _get_formatted_samplesheets(
      samplesheet_file: str,
      runinfo_xml_file: str,
      samplesheet_output_dir: str,
      singlecell_barcode_json: str,
      singlecell_dual_barcode_json: str,
      tenx_sc_tag: str='10X') \
        -> list:
  try:
    check_file_path(samplesheet_file)
    check_file_path(runinfo_xml_file)
    check_file_path(samplesheet_output_dir)
    check_file_path(singlecell_barcode_json)
    check_file_path(singlecell_dual_barcode_json)
    lane_in_samplesheet = True
    formatted_samplesheets_list = list()
    temp_dir = \
      get_temp_dir()
    # 10X sc daul index conversion
    sc_dual_process = \
      ProcessSingleCellDualIndexSamplesheet(
        samplesheet_file=samplesheet_file,
        singlecell_dual_index_barcode_json=singlecell_dual_barcode_json,
        platform='MISEQ',
        index2_rule='NOCHANGE')
    temp_sc_dual_conv_samplesheet_file = \
      os.path.join(temp_dir, 'sc_dual_index_samplesheet.csv')
    sc_dual_process.\
      modify_samplesheet_for_sc_dual_barcode(
        output_samplesheet=temp_sc_dual_conv_samplesheet_file)
    # 10x sc single index conversion
    sc_data = \
      ProcessSingleCellSamplesheet(
        temp_sc_dual_conv_samplesheet_file,
        singlecell_barcode_json,
        tenx_sc_tag)
    temp_sc_conv_samplesheet_file = \
      os.path.join(temp_dir, 'sc_index_samplesheet.csv')
    sc_data.\
      change_singlecell_barcodes(temp_sc_conv_samplesheet_file)
    sa = SampleSheet(temp_sc_conv_samplesheet_file)
    if 'Lane' not in sa._data_header:
      lane_in_samplesheet = False
      ra = RunInfo_xml(runinfo_xml_file)
      lanes_count = \
        ra.get_lane_count()
    # project and lanes
    formatted_project_and_lane = list()
    for row in sa.get_project_and_lane():
      if lane_in_samplesheet:
        (project_name, lane_id) = row.split(':')
        formatted_project_and_lane.\
          append({
            'project_name': project_name.strip(),
            'lane': lane_id.strip()})
      else:
        (project_name,) = row.split(':')
        for lane_id in range(1, lanes_count+1):
          formatted_project_and_lane.\
            append({
              'project_name': project_name.strip(),
              'lane': lane_id})
    # samplesheet group list
    project_counter = 0
    for project_name, p_data in pd.DataFrame(formatted_project_and_lane).groupby('project_name'):
      project_counter += 1
      lane_counter = 0
      for lane_id, _ in p_data.groupby('lane'):
        lane_counter += 1
        sa = SampleSheet(temp_sc_conv_samplesheet_file)
        sa.filter_sample_data(
          condition_key="Sample_Project",
          condition_value=project_name,
          method="include")
        if lane_in_samplesheet:
          sa.filter_sample_data(
            condition_key="Lane",
            condition_value=lane_id,
            method="include")
        ig_counter = 0
        for ig, ig_sa in sa.group_data_by_index_length().items():
          unfiltered_ig_data = deepcopy(ig_sa._data)
          if 'Description' in ig_sa._data_header:
            df = pd.DataFrame(ig_sa._data)
            description_list = \
              df['Description'].\
              map(lambda x: x.upper()).\
              drop_duplicates().\
              values.tolist()
            for desc_item in description_list:
              ig_counter += 1
              ig_sa._data = deepcopy(unfiltered_ig_data)
              ig_sa.filter_sample_data(
                condition_key="Description",
                condition_value=desc_item,
                method="include")
              if desc_item == '':
                desc_item = 'NA'
              samplesheet_name = \
                'SampleSheet_{0}_{1}_{2}_{3}.csv'.\
                format(
                  project_name,
                  lane_id,
                  ig,
                  desc_item)
              ig_samplesheet_temp_path = \
                os.path.join(
                  temp_dir,
                  samplesheet_name)
              ig_samplesheet_path = \
                os.path.join(
                  samplesheet_output_dir,
                  samplesheet_name)
              ig_sa.\
                print_sampleSheet(ig_samplesheet_temp_path)
              bases_mask = \
                _calculate_bases_mask(
                  samplesheet_file=ig_samplesheet_temp_path,
                  runinfoxml_file=runinfo_xml_file,
                  read_offset_cutoff=29)
              ig_final_sa = SampleSheet(ig_samplesheet_temp_path)
              ig_final_sa.\
                set_header_for_bclconvert_run(bases_mask=bases_mask)
              temp_dir = get_temp_dir(use_ephemeral_space=True)
              ig_final_sa.\
                print_sampleSheet(ig_samplesheet_path)
              sample_counts = \
                len(ig_final_sa._data)
              formatted_samplesheets_list.\
                append({
                  'project': project_name,
                  'project_index': project_counter,
                  'lane': lane_id,
                  'lane_index': lane_counter,
                  'bases_mask': bases_mask,
                  'index_group': '{0}_{1}'.format(ig, desc_item),
                  'index_group_index': ig_counter,
                  'sample_counts': sample_counts,
                  'samplesheet_file': ig_samplesheet_path,
                  'output_dir': temp_dir})
          else:
            ig_counter += 1
            samplesheet_name = \
              'SampleSheet_{0}_{1}_{2}.csv'.\
              format(
                project_name,
                lane_id,
                ig)
            ig_samplesheet_temp_path = \
                os.path.join(
                  temp_dir,
                  samplesheet_name)
            ig_samplesheet_path = \
                os.path.join(
                  samplesheet_output_dir,
                  samplesheet_name)
            ig_sa.\
              print_sampleSheet(ig_samplesheet_temp_path)
            bases_mask = \
              _calculate_bases_mask(
                samplesheet_file=ig_samplesheet_temp_path,
                runinfoxml_file=runinfo_xml_file,
                read_offset_cutoff=29)
            ig_final_sa = SampleSheet(ig_samplesheet_temp_path)
            ig_final_sa.\
              set_header_for_bclconvert_run(bases_mask=bases_mask)
            ig_final_sa.\
              print_sampleSheet(ig_samplesheet_path)
            sample_counts = \
              len(ig_final_sa._data)
            temp_dir = get_temp_dir(use_ephemeral_space=True)
            formatted_samplesheets_list.\
              append({
                'project': project_name,
                'project_index': project_counter,
                'lane': lane_id,
                'lane_index': lane_counter,
                'bases_mask': bases_mask,
                'index_group': ig,
                'index_group_index': ig_counter,
                'sample_counts': sample_counts,
                'samplesheet_file': ig_samplesheet_path,
                'output_dir': temp_dir})
    return formatted_samplesheets_list
  except Exception as e:
    raise ValueError(
      f"Failed to get formatted samplesheets and bases mask, error: {e}")


def _calculate_bases_mask(
      samplesheet_file: str,
      runinfoxml_file: str,
      numcycle_label: str='numcycles',
      isindexedread_label: str='isindexedread',
      isreversecomplement_label: str='isreversecomplement',
      read_offset: int=1,
      read_offset_cutoff: int=50) \
        -> str:
  try:
    samplesheet_data = SampleSheet(infile=samplesheet_file)
    index_length_stats = samplesheet_data.get_index_count()
    samplesheet_index_length_list = list()
    for index_name in index_length_stats.keys():
      index_type = len(index_length_stats.get(index_name).keys())
      if index_type > 1:
        raise ValueError(f'column {index_type} has variable lengths')
      index_length = list(index_length_stats.get(index_name).keys())[0]
      samplesheet_index_length_list.\
        append(index_length)
    runinfo_data = RunInfo_xml(xml_file=runinfoxml_file)
    runinfo_reads_stats = runinfo_data.get_reads_stats()
    for read_id in (sorted(runinfo_reads_stats.keys())):
      runinfo_read_length = int(runinfo_reads_stats[read_id].get(numcycle_label))
      if runinfo_reads_stats[read_id][isindexedread_label] == 'N':
        if int(runinfo_read_length) < read_offset_cutoff:
          read_offset = 0
    index_read_position = 0
    bases_mask_list = list()
    for read_id in (sorted(runinfo_reads_stats.keys())):
      runinfo_read_length = int(runinfo_reads_stats[read_id].get(numcycle_label))
      if runinfo_reads_stats[read_id][isindexedread_label] == 'Y':
        samplesheet_index_length = \
          samplesheet_index_length_list[index_read_position]
        index_diff = \
          int(runinfo_read_length) - int(samplesheet_index_length)
        if samplesheet_index_length == 0:
          bases_mask_list.\
            append(f'N{runinfo_read_length}')
          if index_read_position == 0:
            raise ValueError("Index 1 position can't be zero")
        elif index_diff > 0 and \
             samplesheet_index_length > 0:
          if runinfo_reads_stats[read_id].get(isreversecomplement_label) is not None and \
             runinfo_reads_stats[read_id].get(isreversecomplement_label) == 'Y':
            bases_mask_list.\
              append(f'N{index_diff}I{samplesheet_index_length}')
          else:
            bases_mask_list.\
              append(f'I{samplesheet_index_length}N{index_diff}')
        elif index_diff == 0:
          bases_mask_list.\
            append(f'I{samplesheet_index_length}')
        index_read_position += 1
      else:
        if int(read_offset) > 0:
          bases_mask_list.\
            append('Y{0}N{1}'.format(int(runinfo_read_length) - int(read_offset), read_offset))
        else:
          bases_mask_list.\
            append(f'Y{runinfo_read_length}')
    if len(bases_mask_list) < 2:
      raise ValueError("Missing bases mask values")
    return ';'.join(bases_mask_list)
  except Exception as e:
    raise ValueError(
      f"Failed to calculate bases mask, error: {e}")


def get_samplesheet_from_portal_func(**context):
  try:
    ti = context.get('ti')
    samplesheet_xcom_key = \
      context['params'].\
      get('samplesheet_xcom_key', 'samplesheet_data')
    samplesheet_tag = \
      context['params'].\
      get('samplesheet_tag', 'samplesheet_tag')
    samplesheet_file = \
      context['params'].\
      get('samplesheet_file', 'samplesheet_file')
    dag_run = context.get('dag_run')
    seqrun_id = None
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('seqrun_id') is not None:
      seqrun_id = \
        dag_run.conf.get('seqrun_id')
    if seqrun_id is None:
      raise ValueError('seqrun_id is not found in dag_run.conf')
    temp_dir = \
      get_temp_dir(use_ephemeral_space=True)
    seqrun_id_json = \
      os.path.join(temp_dir, 'seqrun_id.json')
    with open(seqrun_id_json, 'w') as fp:
      json.dump({'seqrun_id': seqrun_id}, fp)
    res = \
      upload_files_to_portal(
        url_suffix="/api/v1/raw_seqrun/search_run_samplesheet",
        portal_config_file=IGF_PORTAL_CONF,
        file_path=seqrun_id_json,
        verify=False,
        jsonify=False)
    if res.status_code != 200:
      raise ValueError('Failed to get samplesheet from portal')
    data = res.content.decode('utf-8')
    # deal with runs without valid samplesheets
    if "No samplesheet found" in data:
      raise ValueError(f"No samplesheet found for seqrun_id: {seqrun_id}")
    samplesheet_file = \
      os.path.join(temp_dir, 'SampleSheet.csv')
    with open(samplesheet_file, 'w') as fp:
      fp.write(data)
    samplesheet_tag = None
    if 'Content-Disposition' in res.headers.keys():
      header_message = res.headers.get('Content-Disposition')
      if 'attachment; filename=' in header_message:
        header_message = header_message.replace('attachment; filename=', '')
        samplesheet_tag = header_message.replace(".csv", "")
    if samplesheet_tag is None:
      raise ValueError(f"Failed to get samplesheet from portal")
    ti.xcom_push(
      key=samplesheet_xcom_key,
      value={'samplesheet_tag': samplesheet_tag, 'samplesheet_file': samplesheet_file})
  except Exception as e:
    log.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def mark_seqrun_status_func(**context):
  try:
    # dag_run = context.get('dag_run')
    seqrun_igf_id = \
      context['params'].\
      get('seqrun_igf_id')
    next_task = \
      context['params'].\
      get('next_task', None)
    last_task = \
      context['params'].\
      get('last_task', None)
    seed_status = \
      context['params'].\
      get('seed_status', None)
    no_change_status = \
      context['params'].\
      get('no_change_status', None)
    seed_table = \
      context['params'].\
        get('seed_table', None)
    check_all_pipelines_for_seed_id = \
      context['params'].\
        get('check_all_pipelines_for_seed_id', False)
    # seqrun_id = None
    # if dag_run is not None and \
    #    dag_run.conf is not None and \
    #    dag_run.conf.get('seqrun_id') is not None:
    #   seqrun_id = \
    #     dag_run.conf.get('seqrun_id')
    if seqrun_igf_id is None:
      raise ValueError('seqrun_id is not found')
    status = \
      _check_and_seed_seqrun_pipeline(
        seqrun_id=seqrun_igf_id,
        pipeline_name=context['task'].dag_id,
        dbconf_json_path=DATABASE_CONFIG_FILE,
        seed_status=seed_status,
        seed_table=seed_table,
        no_change_status=no_change_status,
        check_all_pipelines_for_seed_id=check_all_pipelines_for_seed_id)
    if status and \
       next_task is not None:
      return [next_task]
    if not status and \
        last_task is not None:
        return [last_task]
  except Exception as e:
    log.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def find_seqrun_func(**context):
  try:
    # dag_run = context.get('dag_run')
    seqrun_igf_id = \
      context['params'].\
      get('seqrun_igf_id')
    next_task_id = \
      context['params'].\
      get('next_task_id', 'mark_seqrun_as_running')
    no_work_task = \
      context['params'].\
      get('no_work_task', 'no_work')
    task_list = [no_work_task,]
    # if dag_run is not None and \
    #    dag_run.conf is not None and \
    #    dag_run.conf.get('seqrun_id') is not None:
    #   seqrun_id = \
    #     dag_run.conf.get('seqrun_id')
    if seqrun_igf_id is None:
      raise ValueError('seqrun_id is not found')
    seqrun_path = \
      os.path.join(
        HPC_SEQRUN_BASE_PATH,
        seqrun_igf_id)
    run_status = \
      _check_for_required_files(
        seqrun_path,
        file_list=[
          'RunInfo.xml',
          'RunParameters.xml',
          'SampleSheet.csv',
          'Data/Intensities/BaseCalls',
          'InterOp'])
    if run_status:
      _check_and_load_seqrun_to_db(
        seqrun_id=seqrun_igf_id,
        seqrun_path=seqrun_path,
        dbconf_json_path=DATABASE_CONFIG_FILE)
      #seed_status = \
      #  _check_and_seed_seqrun_pipeline(
      #    seqrun_id=seqrun_id,
      #    pipeline_name=context['task'].dag_id,
      #    dbconf_json_path=DATABASE_CONFIG_FILE)
    if run_status:
      task_list = [next_task_id,]
    return task_list
  except Exception as e:
    log.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def _check_for_required_files(seqrun_path: str, file_list: list) -> bool:
  try:
    for file_name in file_list:
      file_path = os.path.join(seqrun_path, file_name)
      if not os.path.exists(file_path):
        return False
    return True
  except Exception as e:
    raise IOError(
      f"Failed to get required files for seqrun {seqrun_path}")


def _check_and_load_seqrun_to_db(
    seqrun_id: str,
    seqrun_path: str,
    dbconf_json_path: str,
    runinfo_file_name: str = 'RunInfo.xml') \
    -> None:
  try:
    dbconf = read_dbconf_json(dbconf_json_path)
    sra = SeqrunAdaptor(**dbconf)
    sra.start_session()
    pl = PlatformAdaptor(**{'session': sra.session})
    run_exists = \
      sra.check_seqrun_exists(seqrun_id)
    if not run_exists:
      runinfo_file = \
        os.path.join(seqrun_path, runinfo_file_name)
      runinfo_data = \
        RunInfo_xml(xml_file=runinfo_file)
      platform_igf_id = \
        runinfo_data.get_platform_number()
      flowcell_id = \
        runinfo_data.get_flowcell_name()
      ## add flowcell details to attribute table
      pl_data = \
        pl.fetch_platform_records_igf_id(
          platform_igf_id=platform_igf_id)
      platform_name = \
        pl_data.model_name
      flowcell_type = ''
      if platform_name == 'HISEQ4000':
        runparameters_file = \
          os.path.join(seqrun_path, 'runParameters.xml')
        runparameters_data = \
          RunParameter_xml(xml_file=runparameters_file)
        flowcell_type = \
          runparameters_data.\
            get_hiseq_flowcell()
      elif platform_name == 'NOVASEQ6000':
        runparameters_file = \
          os.path.join(seqrun_path, 'RunParameters.xml')
        runparameters_data = \
          RunParameter_xml(xml_file=runparameters_file)
        flowcell_type = \
          runparameters_data.\
            get_novaseq_flowcell()
      else:
        flowcell_type = platform_name
      seqrun_data = [{
        'seqrun_igf_id': seqrun_id,
        'platform_igf_id': platform_igf_id,
        'flowcell_id': flowcell_id,
        'flowcell':  flowcell_type}]
      sra.store_seqrun_and_attribute_data(
        data=seqrun_data,
        autosave=True)
    sra.close_session()
  except Exception as e:
    raise ValueError(
      f"Failed to load seqrun {seqrun_id} to database, error: {e}")


def _check_and_seed_seqrun_pipeline(
    seqrun_id: str,
    pipeline_name: str,
    dbconf_json_path: str,
    seed_status: str = 'SEEDED',
    seed_table: str ='seqrun',
    no_change_status: str = 'RUNNING',
    check_all_pipelines_for_seed_id: bool = False) -> bool:
  try:
    dbconf = read_dbconf_json(dbconf_json_path)
    base = BaseAdaptor(**dbconf)
    base.start_session()
    sra = SeqrunAdaptor(**{'session': base.session})
    pa = PipelineAdaptor(**{'session': base.session})
    seqrun_entry = \
      sra.fetch_seqrun_records_igf_id(
          seqrun_igf_id=seqrun_id)
    seed_new_pipeline = True
    if check_all_pipelines_for_seed_id:
      seed_status_list = \
        pa.check_seed_id_status(
          seed_id=seqrun_entry.seqrun_id,
          seed_table=seed_table)
      seed_status_df = \
        pd.DataFrame(seed_status_list)
      if 'SEEDED' in seed_status_df['status'].values.tolist() or \
         'RUNNING' in seed_status_df['status'].values.tolist():
        seed_new_pipeline = False
    if seed_new_pipeline:
      seed_status = \
        pa.create_or_update_pipeline_seed(
          seed_id=seqrun_entry.seqrun_id,
          pipeline_name=pipeline_name,
          new_status=seed_status,
          seed_table=seed_table,
          no_change_status=no_change_status)
    else:
      seed_status = False
    base.close_session()
    return seed_status
  except Exception as e:
    raise ValueError(
      f"Failed to seed pipeline {pipeline_name} for seqrun {seqrun_id}, error: {e}")