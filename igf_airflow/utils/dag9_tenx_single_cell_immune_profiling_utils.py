import os,logging,subprocess,re,fnmatch
import pandas as pd
from copy import copy
from airflow.models import Variable
from igf_airflow.logging.upload_log_msg import log_success,log_failure,log_sleep
from igf_airflow.logging.upload_log_msg import post_image_to_channels
from igf_data.utils.fileutils import get_temp_dir,copy_remote_file,check_file_path
from igf_data.utils.fileutils import read_json_data,copy_local_file,get_date_stamp
from igf_data.utils.singularity_run_wrapper import execute_singuarity_cmd
from igf_data.utils.analysis_fastq_fetch_utils import get_fastq_and_run_for_samples
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.utils.tools.reference_genome_utils import Reference_genome_utils
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.utils.tools.cutadapt_utils import run_cutadapt
from igf_data.utils.tools.cellranger.cellranger_count_utils import run_cellranger_multi
from igf_data.utils.fileutils import create_file_manifest_for_dir
from igf_data.utils.fileutils import prepare_file_archive
from igf_data.utils.analysis_collection_utils import Analysis_collection_utils
from igf_data.igfdb.analysisadaptor import AnalysisAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.utils.igf_irods_client import IGF_irods_uploader
from igf_data.utils.jupyter_nbconvert_wrapper import Notebook_runner
from igf_airflow.logging.upload_log_msg import send_log_to_channels

## DEFAULTS
DATABASE_CONFIG_FILE = Variable.get('test_database_config_file')
SCANPY_SINGLE_SAMPLE_TEMPLATE= Variable.get('scanpy_single_sample_template')
SCANPY_NOTEBOOK_IMAGE = Variable.get('scanpy_notebook_image')
SCIRPY_SINGLE_SAMPLE_TEMPLATE = Variable.get('scirpy_single_sample_template')
SCIRPY_NOTEBOOK_IMAGE = Variable.get('scirpy_notebook_image')
SEURAT_SINGLE_SAMPLE_TEMPLATE = Variable.get('seurat_single_sample_template')
SEURAT_NOTEBOOK_IMAGE = Variable.get('seurat_notebook_image')
CUTADAPT_IMAGE = Variable.get('cutadapt_singularity_image')
MULTIQC_IMAGE = Variable.get('multiqc_singularity_image')
PICARD_IMAGE = Variable.get('picard_singularity_image')
SLACK_CONF = Variable.get('slack_conf')
MS_TEAMS_CONF = Variable.get('ms_teams_conf')
BOX_USERNAME = Variable.get('box_username')
BOX_CONFIG_FILE = Variable.get('box_config_file')
FTP_HOSTNAME = Variable.get('ftp_hostname')
FTP_USERNAME = Variable.get('ftp_username')
FTP_PROJECT_PATH = Variable.get('ftp_project_path')
BASE_RESULT_DIR = Variable.get('base_result_dir')
ALL_CELL_MARKER_LIST = Variable.get('all_cell_marker_list')

## FUNCTION
def task_branch_function(**context):
  try:
    ti = context.get('ti')
    xcom_pull_task_id = \
      context['params'].get('xcom_pull_task_id')
    analysis_info_xcom_key = \
      context['params'].get('analysis_info_xcom_key')
    analysis_name = \
      context['params'].get('analysis_name')
    task_prefix = \
      context['params'].get('task_prefix')
    analysis_info = \
      ti.xcom_pull(
        task_ids=xcom_pull_task_id,
        key=analysis_info_xcom_key)
    sample_info = \
      analysis_info.get(analysis_name)
    run_list = sample_info.get('runs').keys()
    task_list = [
      '{0}_{1}_{2}'.format(task_prefix,analysis_name,run_id)
        for run_id in run_list]
    return task_list
  except Exception as e:
    logging.error(e)
    raise ValueError(e)

def load_analysis_files_func(**context):
  try:
    ti = context.get('ti')
    collection_name_task = \
      context['params'].get('collection_name_task')
    collection_name_key = \
      context['params'].get('collection_name_key')
    file_name_task = \
      context['params'].get('file_name_task')
    file_name_key = \
      context['params'].get('file_name_key')
    analysis_name = \
      context['params'].get('analysis_name')
    collection_type = \
      context['params'].get('collection_type')
    collection_table = \
      context['params'].get('collection_table')
    output_files_key = \
      context['params'].get('output_files_key')
    database_config_file = DATABASE_CONFIG_FILE
    base_result_dir = BASE_RESULT_DIR
    dbparams = \
      read_dbconf_json(database_config_file)
    base = \
      BaseAdaptor(**dbparams)
    tag_name = 'no_tag'
    collection_name = \
      ti.xcom_pull(
        task_ids=collection_name_task,
        key=collection_name_key)
    temp_file = \
      ti.xcom_pull(
        task_ids=file_name_task,
        key=file_name_key)
    if isinstance(temp_file,str):
      temp_file = [temp_file]
    au = \
      Analysis_collection_utils(
        dbsession_class=base.get_session_class(),
        analysis_name=analysis_name,
        tag_name=tag_name,
        collection_name=collection_name,
        collection_type=collection_type,
        collection_table=collection_table,
        base_path=base_result_dir)
    output_file_list = \
      au.load_file_to_disk_and_db(
        input_file_list=temp_file,
        withdraw_exisitng_collection=True)
    ti.xcom_push(
      key=output_files_key,
      value=output_file_list)
  except Exception as e:
    logging.error(e)
    raise ValueError(e)


def run_singlecell_notebook_wrapper_func(**context):
  try:
    ti = context.get('ti')
    cellranger_xcom_key = \
      context['params'].get('cellranger_xcom_key')
    cellranger_xcom_pull_task = \
      context['params'].get('cellranger_xcom_pull_task')
    output_cellbrowser_key = \
      context['params'].get('output_cellbrowser_key')
    timeout = \
      context['params'].get('timeout')
    allow_errors = \
      context['params'].get('allow_errors')
    output_notebook_key = \
      context['params'].get('output_notebook_key')
    count_dir = \
      context['params'].get('count_dir')
    vdj_dir = \
      context['params'].get('vdj_dir')
    analysis_description_xcom_pull_task = \
      context['params'].get('analysis_description_xcom_pull_task')
    analysis_description_xcom_key = \
      context['params'].get('analysis_description_xcom_key')
    kernel_name = \
      context['params'].get('kernel_name')
    analysis_name = \
      context['params'].get('analysis_name')
    analysis_name = analysis_name.upper()
    cell_marker_list = ALL_CELL_MARKER_LIST
    cellranger_output = \
      ti.xcom_pull(
        task_ids=cellranger_xcom_pull_task,
        key=cellranger_xcom_key)
    cellranger_count_dir = \
      os.path.join(cellranger_output,count_dir)
    cellranger_vdj_dir = \
      os.path.join(cellranger_output,vdj_dir)
    dag_run = context.get('dag_run')
    if dag_run is None or \
       dag_run.conf is None or \
       dag_run.conf.get('analysis_id') is None:
      raise ValueError('No analysis id found for collection')
    analysis_id = \
        dag_run.conf.get('analysis_id')
    database_config_file = DATABASE_CONFIG_FILE
    dbparams = \
      read_dbconf_json(database_config_file)
    aa = \
      AnalysisAdaptor(**dbparams)
    aa.start_session()
    project_igf_id = \
      aa.fetch_project_igf_id_for_analysis_id(
        analysis_id=int(analysis_id))
    aa.close_session()
    analysis_description = \
      ti.xcom_pull(
        task_ids=analysis_description_xcom_pull_task,
        key=analysis_description_xcom_key)
    sample_igf_id = \
      analysis_description[0].get('sample_igf_id')
    genome_build = \
      analysis_description[0].get('genome_build')
    tmp_dir = get_temp_dir(use_ephemeral_space=True)
    input_params = {
      'DATE_TAG':get_date_stamp(),
      'PROJECT_IGF_ID':project_igf_id,
      'SAMPLE_IGF_ID':sample_igf_id,
      'CELLRANGER_COUNT_DIR':cellranger_count_dir,
      'CELLRANGER_VDJ_DIR':cellranger_vdj_dir,
      'CELL_MARKER_LIST':cell_marker_list,
      'GENOME_BUILD':genome_build}
    container_bind_dir_list = [
      cellranger_output,
      tmp_dir,
      os.path.dirname(cell_marker_list)]
    if analysis_name == 'SCANPY':
      template_ipynb_path = SCANPY_SINGLE_SAMPLE_TEMPLATE
      singularity_image_path = SCANPY_NOTEBOOK_IMAGE
      scanpy_h5ad = os.path.join(tmp_dir,'scanpy.h5ad')
      cellbrowser_dir = os.path.join(tmp_dir,'cellbrowser_dir')
      if not os.path.exists(cellbrowser_dir):
        os.makedirs(cellbrowser_dir)
      cellbrowser_html_dir = os.path.join(tmp_dir,'cellbrowser_html_dir')
      if not os.path.exists(cellbrowser_html_dir):
        os.makedirs(cellbrowser_html_dir)
      input_params.update({
        'SCANPY_H5AD':scanpy_h5ad,
        'CELLBROWSER_DIR':cellbrowser_dir,
        'CELLBROWSER_HTML_DIR':cellbrowser_html_dir})
    elif analysis_name == 'SCIRPY':
      template_ipynb_path = SCIRPY_SINGLE_SAMPLE_TEMPLATE
      singularity_image_path = SCIRPY_NOTEBOOK_IMAGE
    elif analysis_name == 'SEURAT':
      template_ipynb_path = SEURAT_SINGLE_SAMPLE_TEMPLATE
      singularity_image_path = SEURAT_NOTEBOOK_IMAGE
    else:
      raise ValueError('Analysis name {0} not supported'.format(analysis_name))
    nb = Notebook_runner(
      template_ipynb_path=template_ipynb_path,
      output_dir=tmp_dir,
      input_param_map=input_params,
      container_paths=container_bind_dir_list,
      timeout=timeout,
      kernel=kernel_name,
      singularity_options=['--no-home','-C'],
      allow_errors=allow_errors,
      use_ephemeral_space=True,
      singularity_image_path=singularity_image_path)
    output_notebook_path,_ = \
      nb.execute_notebook_in_singularity()
    ti.xcom_push(
      key=output_notebook_key,
      value=output_notebook_path)
    if analysis_name == 'SCANPY':
      ti.xcom_push(
        key=output_cellbrowser_key,
        value=cellbrowser_html_dir)
  except Exception as e:
    logging.error(e)
    raise ValueError(e)

"""
def run_scanpy_for_sc_5p_func(**context):
  try:
    ti = context.get('ti')
    cellranger_xcom_key = \
      context['params'].get('cellranger_xcom_key')
    cellranger_xcom_pull_task = \
      context['params'].get('cellranger_xcom_pull_task')
    timeout = \
      context['params'].get('scanpy_timeout')
    allow_errors = \
      context['params'].get('allow_errors')
    output_notebook_key = \
      context['params'].get('output_notebook_key')
    output_cellbrowser_key = \
      context['params'].get('output_cellbrowser_key')
    analysis_description_xcom_pull_task = \
      context['params'].get('analysis_description_xcom_pull_task')
    analysis_description_xcom_key = \
      context['params'].get('analysis_description_xcom_key')
    cellranger_output = \
      ti.xcom_pull(
        task_ids=cellranger_xcom_pull_task,
        key=cellranger_xcom_key)
    cellranger_count_dir = \
      os.path.join(cellranger_output,'count')
    tmp_dir = get_temp_dir(use_ephemeral_space=True)
    scanpy_h5ad = os.path.join(tmp_dir,'scanpy.h5ad')
    cellbrowser_dir = os.path.join(tmp_dir,'cellbrowser_dir')
    if not os.path.exists(cellbrowser_dir):
      os.makedirs(cellbrowser_dir)
    cellbrowser_html_dir = os.path.join(tmp_dir,'cellbrowser_html_dir')
    if not os.path.exists(cellbrowser_html_dir):
      os.makedirs(cellbrowser_html_dir)
    template_ipynb_path = SCANPY_SINGLE_SAMPLE_TEMPLATE
    singularity_image_path = SCANPY_NOTEBOOK_IMAGE
    cell_marker_list = ALL_CELL_MARKER_LIST
    dag_run = context.get('dag_run')
    if dag_run is None or \
       dag_run.conf is None or \
       dag_run.conf.get('analysis_id') is None:
      raise ValueError('No analysis id found for collection')
    analysis_id = \
        dag_run.conf.get('analysis_id')
    database_config_file = DATABASE_CONFIG_FILE
    dbparams = \
      read_dbconf_json(database_config_file)
    aa = \
      AnalysisAdaptor(**dbparams)
    aa.start_session()
    project_igf_id = \
      aa.fetch_project_igf_id_for_analysis_id(
        analysis_id=int(analysis_id))
    aa.close_session()
    analysis_description = \
      ti.xcom_pull(
        task_ids=analysis_description_xcom_pull_task,
        key=analysis_description_xcom_key)
    sample_igf_id = \
      analysis_description[0].get('sample_igf_id')
    genome_build = \
      analysis_description[0].get('genome_build')
    input_params = {
      'DATE_TAG':get_date_stamp(),
      'PROJECT_IGF_ID':project_igf_id,
      'SAMPLE_IGF_ID':sample_igf_id,
      'CELLRANGER_COUNT_DIR':cellranger_count_dir,
      'CELL_MARKER_LIST':cell_marker_list,
      'GENOME_BUILD':genome_build,
      'SCANPY_H5AD':scanpy_h5ad,
      'CELLBROWSER_DIR':cellbrowser_dir,
      'CELLBROWSER_HTML_DIR':cellbrowser_html_dir}
    container_bind_dir_list = [
      cellranger_count_dir,
      tmp_dir,
      os.path.dirname(cell_marker_list)]
    nb = Notebook_runner(
      template_ipynb_path=template_ipynb_path,
      output_dir=tmp_dir,
      input_param_map=input_params,
      container_paths=container_bind_dir_list,
      timeout=timeout,
      singularity_options=['--no-home','-C'],
      allow_errors=allow_errors,
      use_ephemeral_space=True,
      singularity_image_path=singularity_image_path)
    output_notebook_path,_ = \
      nb.execute_notebook_in_singularity()
    ti.xcom_push(
      key=output_notebook_key,
      value=output_notebook_path)
    ti.xcom_push(
      key=output_cellbrowser_key,
      value=cellbrowser_html_dir)
  except Exception as e:
    logging.error(e)
    raise ValueError(e)
"""

def irods_files_upload_for_analysis(**context):
  try:
    ti = context.get('ti')
    xcom_pull_task = \
      context['params'].get('xcom_pull_task')
    xcom_pull_files_key = \
      context['params'].get('xcom_pull_files_key')
    collection_name_key = \
      context['params'].get('collection_name_key')
    irods_exe_dir = \
      Variable.get('irods_exe_dir')
    analysis_name = \
      context['params'].get('analysis_name')
    dag_run = context.get('dag_run')
    if dag_run is None or \
       dag_run.conf is None or \
       dag_run.conf.get('analysis_id') is None:
      raise ValueError('No analysis id found for collection')
    analysis_id = \
        dag_run.conf.get('analysis_id')
    database_config_file = \
      DATABASE_CONFIG_FILE
    dbparams = \
      read_dbconf_json(database_config_file)
    aa = \
      AnalysisAdaptor(**dbparams)
    aa.start_session()
    project_igf_id = \
      aa.fetch_project_igf_id_for_analysis_id(
        analysis_id=int(analysis_id))
    aa.close_session()
    file_list_for_copy = \
      ti.xcom_pull(
        task_ids=xcom_pull_task,
        key=xcom_pull_files_key)
    collection_name = \
      ti.xcom_pull(
        task_ids=xcom_pull_task,
        key=collection_name_key)
    pa = ProjectAdaptor(**dbparams)
    pa.start_session()
    user = \
      pa.fetch_data_authority_for_project(
        project_igf_id=project_igf_id)                                        # fetch user info from db
    pa.close_session()
    if user is None:
        raise ValueError(
                'No user found for project {0}'.\
                  format(project_igf_id))
    username = user.username                                                  # get username for irods
    irods_upload = IGF_irods_uploader(irods_exe_dir)
    for file in file_list_for_copy:
      check_file_path(file)
    dir_path_list = ['analysis',collection_name]
    irods_upload.\
      upload_analysis_results_and_create_collection(
        file_list=file_list_for_copy,
        irods_user=username,
        project_name=project_igf_id,
        analysis_name=analysis_name,
        dir_path_list=dir_path_list,
        file_tag=collection_name)
  except Exception as e:
    logging.error(e)
    raise ValueError(e)


def ftp_files_upload_for_analysis(**context):
  try:
    ti = context.get('ti')
    xcom_pull_task = \
      context['params'].get('xcom_pull_task')
    xcom_pull_files_key = \
      context['params'].get('xcom_pull_files_key')
    collection_name_task = \
      context['params'].get('collection_name_task')
    collection_name_key = \
      context['params'].get('collection_name_key')
    collection_type = \
      context['params'].get('collection_type')
    collection_table = \
      context['params'].get('collection_table')
    collect_remote_file = \
      context['params'].get('collect_remote_file')
    dag_run = context.get('dag_run')
    if dag_run is None or \
       dag_run.conf is None or \
       dag_run.conf.get('analysis_id') is None:
      raise ValueError('No analysis id found for collection')
    analysis_id = \
        dag_run.conf.get('analysis_id')
    database_config_file = DATABASE_CONFIG_FILE
    dbparams = \
      read_dbconf_json(database_config_file)
    aa = \
      AnalysisAdaptor(**dbparams)
    aa.start_session()
    project_igf_id = \
      aa.fetch_project_igf_id_for_analysis_id(analysis_id=int(analysis_id))
    aa.close_session()
    ftp_hostname = FTP_HOSTNAME
    ftp_username = FTP_USERNAME
    ftp_project_path = FTP_PROJECT_PATH
    file_list_for_copy = \
      ti.xcom_pull(
        task_ids=xcom_pull_task,
        key=xcom_pull_files_key)
    if isinstance(file_list_for_copy,str):
      file_list_for_copy = [file_list_for_copy]
    collection_name = \
      ti.xcom_pull(
        task_ids=collection_name_task,
        key=collection_name_key)
    destination_output_path = \
      os.path.join(
        ftp_project_path,
        project_igf_id,
        'analysis',
        collection_name)
    output_file_list = list()
    temp_work_dir = \
      get_temp_dir(use_ephemeral_space=False)
    for file in file_list_for_copy:
      check_file_path(file)
      if os.path.isfile(file):
        copy_local_file(
          file,
          os.path.join(
            temp_work_dir,
            os.path.basename(file)))
        dest_file_path = \
          os.path.join(
            destination_output_path,
            os.path.basename(file))
        os.chmod(
          os.path.join(
            temp_work_dir,
            os.path.basename(file)),
          mode=0o764)
      elif os.path.isdir(file):
        copy_local_file(
          file,
          os.path.join(
            temp_work_dir,
            os.path.basename(file)))
        dest_file_path = destination_output_path
        for root,dirs,files in os.walk(temp_work_dir):
          for dir_name in dirs:
            os.chmod(
              os.path.join(root,dir_name),
              mode=0o775)
          for file_name in files:
            os.chmod(
              os.path.join(root,file_name),
              mode=0o764)
      else:
        raise ValueError(
                'Unknown source file type: {0}'.\
                  format(file))
      copy_remote_file(
        source_path=os.path.join(temp_work_dir,
                                 os.path.basename(file)),
        destinationa_path=dest_file_path,
        destination_address='{0}@{1}'.format(ftp_username,ftp_hostname),
        force_update=True)
      if os.path.isdir(file):
        dest_file_path = \
          os.path.join(
            dest_file_path,
            os.path.basename(file))
      output_file_list.append(dest_file_path)
    remove_dir(dir_path=temp_work_dir)
    if collect_remote_file:
      data = list()
      remove_data_list = [{
        'name':collection_name,
        'type':collection_type}]
      for file in output_file_list:
        data.append({
          'name':collection_name,
          'type':collection_type,
          'table':collection_table,
          'file_path':file,
          'location':'HPC_PROJECT'})
        ca = CollectionAdaptor(**dbparams)
        ca.start_session()
        try:
          ca.remove_collection_group_info(
            data=remove_data_list,
            autosave=False)                                                     # remove existing data before loading new collection
          ca.load_file_and_create_collection(
            data=data,
            autosave=False,
            calculate_file_size_and_md5=False)                                  # load remote files to db
          ca.commit_session()                                                   # commit changes
          ca.close_session()
        except:
          ca.rollback_session()                                                 # rollback changes
          ca.close_session()
          raise
  except Exception as e:
    logging.error(e)
    raise ValueError(e)


def load_cellranger_result_to_db_func(**context):
  try:
    ti = context.get('ti')
    cellranger_xcom_key = \
      context['params'].get('cellranger_xcom_key')
    cellranger_xcom_pull_task = \
      context['params'].get('cellranger_xcom_pull_task')
    analysis_description_xcom_pull_task = \
      context['params'].get('analysis_description_xcom_pull_task')
    analysis_description_xcom_key = \
      context['params'].get('analysis_description_xcom_key')
    analysis_name = \
      context['params'].get('analysis_name')
    collection_table = \
      context['params'].get('collection_table')
    collection_type = \
      context['params'].get('collection_type')
    genome_column = \
      context['params'].get('genome_column')
    output_xcom_key = \
      context['params'].get('output_xcom_key')
    xcom_collection_name_key = \
      context['params'].get('xcom_collection_name_key')
    html_xcom_key = \
      context['params'].get('html_xcom_key')
    html_report_file_name = \
      context['params'].get('html_report_file_name')
    analysis_description = \
      ti.xcom_pull(
        task_ids=analysis_description_xcom_pull_task,
        key=analysis_description_xcom_key)
    sample_igf_id = \
      analysis_description[0].get('sample_igf_id')
    genome_build = \
      analysis_description[0].get(genome_column)
    if sample_igf_id is None:
      raise ValueError(
              'No sample id found for analysis {0}'.\
                format(analysis_description))
    cellranger_output = \
      ti.xcom_pull(
        task_ids=cellranger_xcom_pull_task,
        key=cellranger_xcom_key)
    html_report_filepath = \
      os.path.join(
        cellranger_output,
        html_report_file_name)
    check_file_path(html_report_filepath)
    manifest_file = \
      os.path.join(
        cellranger_output,
        'file_manifest.csv')
    create_file_manifest_for_dir(
      results_dirpath=cellranger_output,
      output_file=manifest_file,
      md5_label='md5',
      exclude_list=['*.bam','*.bai','*.cram'])
    temp_archive_name = \
      os.path.join(
        get_temp_dir(use_ephemeral_space=False),
        '{0}.tar.gz'.format(sample_igf_id))
    prepare_file_archive(
      results_dirpath=cellranger_output,
      output_file=temp_archive_name,
      exclude_list=['*.bam','*.bai','*.cram'])
    base_result_dir = BASE_RESULT_DIR
    database_config_file = DATABASE_CONFIG_FILE
    dbparams = \
      read_dbconf_json(database_config_file)
    base = \
      BaseAdaptor(**dbparams)
    au = \
      Analysis_collection_utils(
        dbsession_class=base.get_session_class(),
        analysis_name=analysis_name,
        tag_name=genome_build,
        collection_name=sample_igf_id,
        collection_type=collection_type,
        collection_table=collection_table,
        base_path=base_result_dir)
    output_file_list = \
      au.load_file_to_disk_and_db(
        input_file_list=[temp_archive_name],
        withdraw_exisitng_collection=True)
    ti.xcom_push(
      key=output_xcom_key,
      value=output_file_list)
    ti.xcom_push(
      key=xcom_collection_name_key,
      value=sample_igf_id)
    ti.xcom_push(
      key=html_xcom_key,
      value=html_report_filepath)
  except Exception as e:
    logging.error(e)
    raise ValueError(e)


def decide_analysis_branch_func(**context):
  try:
    ti = context.get('ti')
    library_csv_xcom_pull_task = \
      context['params'].get('library_csv_xcom_pull_task')
    library_csv_xcom_key = \
      context['params'].get('library_csv_xcom_key')
    load_cellranger_result_to_db_task = \
      context['params'].get('load_cellranger_result_to_db_task')
    run_scanpy_for_sc_5p_task = \
      context['params'].get('run_scanpy_for_sc_5p_task')
    run_scirpy_for_vdj_task = \
      context['params'].get('run_scirpy_for_vdj_task')
    run_scirpy_for_vdj_b_task = \
      context['params'].get('run_scirpy_for_vdj_b_task')
    run_scirpy_vdj_t_task = \
      context['params'].get('run_scirpy_vdj_t_task')
    run_seurat_for_sc_5p_task = \
      context['params'].get('run_seurat_for_sc_5p_task')
    run_picard_alignment_summary_task = \
      context['params'].get('run_picard_alignment_summary_task')
    convert_bam_to_cram_task = \
      context['params'].get('convert_bam_to_cram_task')
    task_list = [load_cellranger_result_to_db_task]
    library_csv = \
      ti.xcom_pull(
        task_ids=library_csv_xcom_pull_task,
        key=library_csv_xcom_key)
    feature_list = \
      _get_feature_list_from_lib_csv(library_csv)
    if feature_list is None or \
       len(feature_list) == 0:
      raise ValueError(
              'No features found in file {0}'.\
                format(library_csv))
    if 'gene_expression' in feature_list:
      task_list.\
        append(run_scanpy_for_sc_5p_task)
      task_list.\
        append(run_seurat_for_sc_5p_task)
      task_list.\
        append(run_picard_alignment_summary_task)
      task_list.\
        append(convert_bam_to_cram_task)
    if 'vdj' in feature_list:
      task_list.\
        append(run_scirpy_for_vdj_task)
    if 'vdj-b' in feature_list:
      task_list.\
        append(run_scirpy_for_vdj_b_task)
    if 'vdj-t' in feature_list:
      task_list.\
        append(run_scirpy_vdj_t_task)
    return task_list
  except Exception as e:
    logging.error(e)
    raise ValueError(e)


def _get_feature_list_from_lib_csv(library_csv):
  try:
    check_file_path(library_csv)
    data = list()
    columns = list()
    feature_list = list()
    with open(library_csv,'r') as fp:
      lib_data = 0
      for line in fp:
        line = line.strip()
        if lib_data == 1:
          if line.startswith('fastq_id'):
            columns = line.split(',')
          else:
            data.extend([line.split(',')])
        if line.startswith('[libraries]'):
          lib_data = 1
    df = pd.DataFrame(data,columns=columns)
    feature_list = \
      list(df['feature_types'].\
           map(lambda x: x.lower().replace(' ','_')).\
           drop_duplicates().values)
    return feature_list
  except Exception as e:
    raise ValueError(e)


def run_cellranger_tool(**context):
  try:
    ti = context.get('ti')
    analysis_description_xcom_pull_task = \
      context['params'].get('analysis_description_xcom_pull_task')
    analysis_description_xcom_key = \
      context['params'].get('analysis_description_xcom_key')
    library_csv_xcom_key = \
      context['params'].get('library_csv_xcom_key')
    library_csv_xcom_pull_task = \
      context['params'].get('library_csv_xcom_pull_task')
    cellranger_xcom_key = \
      context['params'].get('cellranger_xcom_key')
    cellranger_options = \
      context['params'].get('cellranger_options')
    analysis_description = \
      ti.xcom_pull(
        task_ids=analysis_description_xcom_pull_task,
        key=analysis_description_xcom_key)
    library_csv = \
      ti.xcom_pull(
        task_ids=library_csv_xcom_pull_task,
        key=library_csv_xcom_key)
    cellranger_exe = Variable.get('cellranger_exe')
    job_timeout = Variable.get('cellranger_job_timeout')
    output_dir = get_temp_dir(use_ephemeral_space=True)
    sample_id = None
    for entry in analysis_description:
      sample_igf_id = entry.get('sample_igf_id')
      if sample_id is None:
        sample_id = sample_igf_id
      else:
        sample_id = '{0}_{1}'.format(sample_id,sample_igf_id)
    _,output_dir = \
      run_cellranger_multi(
        cellranger_exe=cellranger_exe,
        library_csv=library_csv,
        sample_id=sample_id,
        output_dir=output_dir,
        use_ephemeral_space=False,
        job_timeout=job_timeout,
        cellranger_options=cellranger_options)
    ti.xcom_push(
      key=cellranger_xcom_key,
      value=output_dir)
  except Exception as e:
    logging.error(e)
    raise ValueError(e)


def run_sc_read_trimmming_func(**context):
  try:
    ti = context.get('ti')
    xcom_pull_task_id = \
      context['params'].get('xcom_pull_task_id')
    analysis_info_xcom_key = \
      context['params'].get('analysis_info_xcom_key')
    analysis_description_xcom_key = \
      context['params'].get('analysis_description_xcom_key')
    analysis_name = \
      context['params'].get('analysis_name')
    run_id = \
      context['params'].get('run_id')
    r1_length = \
      context['params'].get('r1_length')
    r2_length = \
      context['params'].get('r2_length')
    fastq_input_dir_tag = \
      context['params'].get('fastq_input_dir_tag')
    fastq_output_dir_tag = \
      context['params'].get('fastq_output_dir_tag')
    singularity_image = CUTADAPT_IMAGE
    analysis_info = \
      ti.xcom_pull(
        task_ids=xcom_pull_task_id,
        key=analysis_info_xcom_key)
    analysis_description = \
      ti.xcom_pull(
        task_ids=xcom_pull_task_id,
        key=analysis_description_xcom_key)
    _get_fastq_and_run_cutadapt_trim(
      analysis_info=analysis_info,
      analysis_name=analysis_name,
      analysis_description=analysis_description,
      run_id=run_id,
      r1_length=r1_length,
      r2_length=r2_length,
      fastq_input_dir_tag=fastq_input_dir_tag,
      fastq_output_dir_tag=fastq_output_dir_tag,
      singularity_image=singularity_image)
  except Exception as e:
    logging.error(e)
    raise ValueError(e)


def _get_fastq_and_run_cutadapt_trim(
      analysis_info,analysis_description,analysis_name,
      run_id,fastq_input_dir_tag,fastq_output_dir_tag,
      singularity_image,r1_length=0,r2_length=0,
      cutadapt_exe='cutadapt',dry_run=False,
      cutadapt_options=('--cores=1',)):
  """
  An internal method for trimming or copying fastq files for Cellranger run

  :param analysis_info: Analysis info dictionary object
  :param analysis_description: Analysis description list of dictionary object
  :param analysis_name: Analysis name to fetch data from analysis info
  :param run_id: Run id to fetch fastq paths from analysis info
  :param r1_length: Trimmed length for R1 fastq, default 0
  :param r2_length: Trimmed length for R2 fastq, default 0
  :param fastq_input_dir_tag: Fastq input dir tag for analysis info lookup
  :param fastq_output_dir_tag: Fastq output dir tag for analysis info lookup
  :param singularity_image: Singularity image path for cutadapt tool
  :param cutadapt_exe: Cutadapt exe path, default cutadapt
  :param dry_run: A toggle for dry run, default False
  :param cutadapt_options: Cutadapt run options, default ('--cores=1',)
  :returns: None
  """
  try:
    sample_info = \
      analysis_info.get(analysis_name)
    if sample_info is None:
      raise ValueError(
              'No feature {0} found in analysis_info'.\
                format(analysis_info))
    sample_igf_id = sample_info.get('sample_igf_id')
    analysis_description = pd.DataFrame(analysis_description).fillna(0)
    analysis_entry = \
      analysis_description[analysis_description['sample_igf_id']==sample_igf_id].copy()
    if 'r1_length' in analysis_entry.columns:
      r1_length = analysis_entry['r1_length'].values[0]                         # reset r1 length
    if 'r2_length' in analysis_entry.columns:
      r2_length = analysis_entry['r2_length'].values[0]                         # reset r2 length
    run = sample_info.get('runs').get(str(run_id))
    if run is None:
      raise ValueError(
              'No run {0} found for feature {1} in analysis_info'.\
                format(run,analysis_name))
    if isinstance(cutadapt_options,tuple):
      cutadapt_options = list(cutadapt_options)
    input_fastq_dir = run.get(fastq_input_dir_tag)
    output_fastq_dir = run.get(fastq_output_dir_tag)
    r1_file_name_pattern = \
      re.compile(r'(\S+)_S\d+_L00\d_R1_001\.fastq\.gz')
    r2_file_name_pattern = \
      re.compile(r'(\S+)_S\d+_L00\d_R2_001\.fastq\.gz')
    index_file_name_pattern = \
      re.compile(r'(\S+)_S\d+_L00\d_I(\d)_001\.fastq\.gz')
    for fastq in os.listdir(input_fastq_dir):
      if fnmatch.fnmatch(fastq,'*.fastq.gz'):
        input_fastq_file = \
          os.path.join(input_fastq_dir,fastq)
        output_fastq_file = \
          os.path.join(output_fastq_dir,fastq)
        if re.match(r1_file_name_pattern,fastq):
          # trim R1
          if r1_length > 0:
            cutadapt_options_r1 = None
            if len(cutadapt_options) >0 :
              cutadapt_options_r1 = copy(cutadapt_options)
              cutadapt_options_r1.\
                append('-l {0}'.format(r1_length))
            _ = \
              run_cutadapt(
                read1_fastq_in=input_fastq_file,
                read1_fastq_out=output_fastq_file,
                cutadapt_options=cutadapt_options_r1,
                cutadapt_exe=cutadapt_exe,
                dry_run=dry_run,
                singularity_image_path=singularity_image)
          else:
            copy_local_file(
              input_fastq_file,
              output_fastq_file)
        if re.match(r2_file_name_pattern,fastq):
          # trim R2
          if r2_length > 0:
            cutadapt_options_r2 = None
            if len(cutadapt_options) >0 :
              cutadapt_options_r2 = copy(cutadapt_options)
              cutadapt_options_r2.\
                append('-l {0}'.format(r2_length))
            _ = \
              run_cutadapt(
                read1_fastq_in=input_fastq_file,
                read1_fastq_out=output_fastq_file,
                cutadapt_options=cutadapt_options_r2,
                cutadapt_exe=cutadapt_exe,
                dry_run=dry_run,
                singularity_image_path=singularity_image)
          else:
            copy_local_file(
              input_fastq_file,
              output_fastq_file)
        if re.match(index_file_name_pattern,fastq):
          # copy I1 or I2
          copy_local_file(
            input_fastq_file,
            output_fastq_file)
  except Exception as e:
    raise ValueError(
            'Failed to trim or copy reads, error: {0}'.\
              format(e))


def configure_cellranger_run_func(**context):
  try:
    ti = context.get('ti')
    # xcop_pull analysis_description
    # xcom_pull analysis_info
    xcom_pull_task_id = \
      context['params'].get('xcom_pull_task_id')
    analysis_description_xcom_key = \
      context['params'].get('analysis_description_xcom_key')
    analysis_info_xcom_key = \
      context['params'].get('analysis_info_xcom_key')
    library_csv_xcom_key = \
      context['params'].get('library_csv_xcom_key')
    work_dir = \
      get_temp_dir(use_ephemeral_space=True)
    analysis_description = \
      ti.xcom_pull(
        task_ids=xcom_pull_task_id,
        key=analysis_description_xcom_key)
    analysis_info = \
      ti.xcom_pull(
        task_ids=xcom_pull_task_id,
        key=analysis_info_xcom_key)
    # generate library.csv file for cellranger run
    csv_path = \
      _create_library_csv_for_cellranger_multi(
        analysis_description=analysis_description,
        analysis_info=analysis_info,
        work_dir=work_dir)
    # push the csv path to xcom
    ti.xcom_push(
      key=library_csv_xcom_key,
      value=csv_path)
  except Exception as e:
    logging.error(e)
    raise ValueError(e)


def _get_data_for_csv_header_section(analysis_entry,column_list):
  try:
    data = dict()
    for col in column_list:
      if col in analysis_entry and \
         analysis_entry.get(col) is not None:
        data.update({col:analysis_entry.get(col)})
    return data
  except:
    raise

def _create_library_csv_for_cellranger_multi(analysis_description,analysis_info,work_dir):
  try:
    header_section_columns = {
      'gene-expression' : [
        'reference',
        'target-panel',
        'no-target-umi-filter',
        'r1-length',
        'r2-length',
        'chemistry',
        'expect-cells',
        'force-cells',
        'include-introns'],
      'feature':[
        'reference',
        'r1-length',
        'r2-length'],
      'vdj':[
        'reference',
        'inner-enrichment-primers',
        'r1-length',
        'r2-length']}
    library_cols = [
      'fastq_id',
      'fastqs',
      'lanes',
      'feature_types',
      'subsample_rate']
    header_data = dict()
    for analysis_entry in analysis_description:
      feature_type = analysis_entry.get('feature_type')
      feature_type = feature_type.replace(' ','_').lower()
      header_section = None
      if feature_type == 'gene_expression':
        header_section = 'gene-expression'
      elif feature_type in ('vdj','vdj-t','vdj-b'):
        header_section = 'vdj'
      elif feature_type in ("antibody_capture","antigen_capture","crisper_guide_capture"):
        header_section = 'feature'
      column_list = header_section_columns.get(header_section)
      data = \
        _get_data_for_csv_header_section(
          analysis_entry,
          column_list)
      if header_section is None:
        raise ValueError(
                'Failed to get header section for {0}'.\
                  format(analysis_entry))
      header_data.\
        update({header_section:data})
    library_data = list()
    for feature_name,feature_data in analysis_info.items():
      fastq_id = feature_data.get('sample_name')
      feature_name = feature_name.replace('_',' ')
      runs = feature_data.get('runs')
      for _,run_data in runs.items():
        fastqs = run_data.get('output_path')
        if fastq_id is None or \
           feature_name is None or \
           fastqs is None:
          raise ValueError(
                  'Misssing data: {0}, {1}'.\
                    format(feature_name,feature_data))
        library_data.\
          append({
            'fastq_id':fastq_id,
            'fastqs':fastqs,
            'lanes':'',
            'feature_types':feature_name,
            'subsample_rate':''
          })
    output_csv = os.path.join(work_dir,'library.csv')
    with open(output_csv,'w') as fp:
      for header_key,data in header_data.items():
        fp.write('[{0}]\n'.format(header_key))
        for data_key,data_value in data.items():
          fp.write('{0},{1}\n'.format(data_key,data_value))
      fp.write('[libraries]\n')
      fp.write('{0}\n'.format(','.join(library_cols)))
      for entry in library_data:
        data_col = [
          entry.get(col)
            for col in library_cols]
        fp.write('{0}\n'.format(','.join(data_col)))
    return output_csv
  except Exception as e:
    raise ValueError(
            'Failed to create cellranger config file for run, error: {0}'.\
              format(e))


def fetch_analysis_info_and_branch_func(**context):
  """
  Fetch dag_run.conf and analysis_description
  """
  try:
    analysis_list = list()
    dag_run = context.get('dag_run')
    ti = context.get('ti')
    no_analysis = \
      context['params'].get('no_analysis_task')
    analysis_description_xcom_key = \
      context['params'].get('analysis_description_xcom_key')
    analysis_info_xcom_key = \
      context['params'].get('analysis_info_xcom_key')
    database_config_file = \
      DATABASE_CONFIG_FILE
    analysis_list.append(no_analysis)
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('analysis_description') is not None:
      analysis_description = \
        dag_run.conf.get('analysis_description')
      analysis_id = \
        dag_run.conf.get('analysis_id')
      analysis_type = \
        dag_run.conf.get('analysis_type')
      feature_types = \
        Variable.get('tenx_single_cell_immune_profiling_feature_types').split(',')
      # add reference genome paths if reference type and genome build is present
      # check for genome build info
      # INPUT:
      # analysis_description = [{
      #   'sample_igf_id':'IGF001',
      #   'feature_type':'gene expression',
      #   'reference_type':'TRANSCRIPTOME_TENX',
      #   'r1_length':26,
      #   'r2_length':0,                          # optional, 0 for no trimming
      #   'cell_annotation_csv':'/path/csv',      # optional, cell annotation file
      #   'genome_build':'HG38' }]
      #  OUTPUT:
      # analysis_description = [{
      #   'sample_igf_id':'IGF001',
      #   'feature_type':'gene expression',
      #   'reference_type':'TRANSCRIPTOME_TENX',
      #   'r1_length':26,
      #   'r2_length':0,                          # optional, 0 for no trimming
      #   'cell_annotation_csv':'/path/csv',      # optional, cell annotation file
      #   'reference':'/path/ref',
      #   'genome_build':'HG38' }]
      analysis_description = \
        _add_reference_genome_path_for_analysis(
          database_config_file=database_config_file,
          analysis_description=analysis_description,
          genome_required=True)
      # check the analysis description and sample validity
      # warn if multiple samples are allocated to same sub category
      # filter analysis branch list
      # INPUT: formatted analysis description with reference column
      # OUTPUT: a list of samples, a list of analysis and a list of errors
      sample_id_list, analysis_list, messages = \
        _validate_analysis_description(
          analysis_description=analysis_description,
          feature_types=feature_types)
      if len(messages) > 0:
        raise ValueError('Analysis validation failed: {0}'.\
                format(messages))
      # get the fastq paths for sample ids and set the trim output dirs per run
      fastq_list = \
        get_fastq_and_run_for_samples(
          dbconfig_file=database_config_file,
          sample_igf_id_list=sample_id_list,
          active_status='ACTIVE',
          combine_fastq_dir=False)
      #
      # INPUT:
      # formatted analysis description = [{
      #   'sample_igf_id':'IGF001',
      #   'feature_type':'gene expression',
      #   'reference_type':'TRANSCRIPTOME_TENX',
      #   'r1_length':26,
      #   'r2_length':0,                          # optional, 0 for no trimming
      #   'cell_annotation_csv':'/path/csv',      # optional, cell annotation file
      #   'reference':'/path/ref',
      #   'genome_build':'HG38' }]
      #
      # list of dictionary containing the sample_igf_id, run_igf_id and fastq file_paths
      # fastq_run_list = [{
      #   'sample_igf_id':'IGF001',
      #   'run_igf_id':'run1',
      #   'file_path':'/path/IGF001/run1/IGF001-GEX_S1_L001_R1_001.fastq.gz' },{
      #   'sample_igf_id':'IGF001',
      #   'run_igf_id':'run1',
      #   'file_path':'/path/IGF001/run1/IGF001-GEX_S1_L001_R2_001.fastq.gz' }]
      #
      # OUTPUT:
      # analysis_info = {
      #   gene_expression:{
      #     sample_igf_id: 'IGF001'
      #     sample_name: 'Sample_XYZ'
      #     run_count: 1,
      #     runs:[{
      #       run_igf_id: 'run_01',
      #       fastq_dir: '/path/input',
      #       output_path: '/path/output' }]
      #   }}
      #
      analysis_info = \
        _fetch_formatted_analysis_description(
          analysis_description,
          fastq_list)
      if len(messages) > 0:
        raise ValueError('Analysis description formatting failed: {0}'.\
                format(messages))
      # mark analysis_id as running,if its not already running
      status = \
        _check_and_mark_analysis_seed(
          analysis_id=analysis_id,
          anslysis_type=analysis_type,
          new_status='RUNNING',
          no_change_status='FINISHED',
          database_config_file=database_config_file)
      # xcom push analysis_info and analysis_description
      if status:
        ti.xcom_push(
          key=analysis_description_xcom_key,
          value=analysis_description)
        ti.xcom_push(
          key=analysis_info_xcom_key,
          value=analysis_info)
      else:
        analysis_list = [no_analysis]                                           # reset analysis list with no_analysis
    return analysis_list                                                        # return analysis list for branching
  except Exception as e:
    logging.error(e)
    raise ValueError(e)


def _add_reference_genome_path_for_analysis(
      database_config_file,analysis_description,genome_column='genome_build',
      genome_required=True,reference_column='reference',reference_type_column='reference_type'):
  try:
    dbparams = read_dbconf_json(database_config_file)
    base = BaseAdaptor(**dbparams)
    messages = list()
    for entry in analysis_description:
      reference = entry.get(reference_column)
      genome_build = entry.get(genome_column)
      reference_type = entry.get(reference_type_column)
      if genome_build is None and \
         genome_required:
        raise ValueError(
                'No genome build info present: {0}'.\
                  format(entry))
      if reference is None:
        if genome_build is None or \
           reference_type is None:
           messages.\
             append('No reference infor for entry {0}'.\
                format(entry))
        else:
          ref_tool = \
            Reference_genome_utils(
              dbsession_class=base.get_session_class(),
              genome_tag=genome_build)
          reference = \
            ref_tool.\
              _fetch_collection_files(
                collection_type=reference_type,
                check_missing=False)
          if reference is None:
            messages.\
              append('Failed to fetch ref from db for entry {0}'.\
                format(entry))
          entry.\
            update({
              reference_column:reference})
    if len(messages) > 0:
      raise ValueError(
              'List of errors for reference lookup: {0}'.\
                format(messages))
    return analysis_description
  except Exception as e:
    raise ValueError(
            'Failed to fetch reference genome, error: {0}'.\
              format(e))


def _check_and_mark_analysis_seed(
      analysis_id,anslysis_type,database_config_file,new_status='RUNNING',
      analysis_table='analysis',no_change_status='RUNNING'):
  """
  Mark pipeline seed as running for analysis

  :param analysis_id: Analysis id to mark in pipeline_seed table
  :param anslysis_type: Analysis type to select Pipeline name
  :param database_config_file: Database config file for dbconnection
  :param new_status: Running tag for pipelien_seed table, default 'RUNNING'
  :param analysis_table: Analsysis table name, default 'analysis'
  :returns: Boolean change status
  """
  try:
    dbparam = \
      read_dbconf_json(database_config_file)
    pl = \
      PipelineAdaptor(**dbparam)
    pl.start_session()
    try:
      status = \
        pl.create_or_update_pipeline_seed(
          seed_id=analysis_id,
          pipeline_name=anslysis_type,
          new_status=new_status,
          seed_table=analysis_table,
          no_change_status=no_change_status,
          autosave=False)
      pl.commit_session()
      pl.close_session()
    except Exception as e:
      pl.rollback_session()
      pl.close_session()
      raise ValueError(
        'Failed to change seeds in db, error: {0}'.format(e))
    return status
  except Exception as e:
    raise ValueError(e)


def _fetch_formatted_analysis_description(
      analysis_description,fastq_run_list,feature_column='feature_type',
      sample_column='sample_igf_id',run_column='run_igf_id',file_column='file_path'):
  """
  A function for formatting analysis description with fastq paths

  :param analysis_description: A list of dictionary containing analysis description
  :param fastq_run_list: A list of dictionary containg fastq file paths
  :param feature_column: Feature column in analysis description, default 'feature_type'
  :param sample_column: Sample column name in analysis description and fastq list, default 'sample_igf_id'
  :param run_column: Run column name in fastq list, default 'run_igf_id'
  :param file_column: File column name in fastq list, default 'file_path'
  :returns: A list of analysis description with run details

  analysis_description = [{
    'sample_igf_id':'IGF001',
    'sample_name': 'sample_name',
    'feature_type':'gene expression',
    'reference_type':'TRANSCRIPTOME_TENX',
    'reference':'/path/ref',
    'genome_build':'HG38' }]

  formatted_analysis_description = {
    gene_expression:{
      sample_igf_id: 'IGF001'
      sample_name: 'Sample_XYZ'
      run_count: 1,
      runs:[{
        run_igf_id: 'run_01',
        fastq_dir: '/path/input',
        output_path: '/path/output' }]
    }}
  """
  try:
    formatted_analysis_description = dict()
    analysis_description_df = pd.DataFrame(analysis_description)
    fastq_run_list_df = pd.DataFrame(fastq_run_list)
    fastq_run_list_df['fastq_dir'] = \
      fastq_run_list_df[file_column].\
        map(lambda x: os.path.dirname(x))
    tmp_dir = get_temp_dir(use_ephemeral_space=True)
    for feature,f_data in analysis_description_df.groupby(feature_column):
      feature = \
        feature.replace(' ','_').\
        lower()
      sample_igf_id = \
        list(f_data[sample_column].values)[0]
      sample_records = \
        fastq_run_list_df[fastq_run_list_df[sample_column]==sample_igf_id]
      if len(sample_records.index)==0:
        raise ValueError(
                'No records found for sample: {0}, feature: {1}'.\
                  format(sample_igf_id,feature))
      total_runs_for_sample = \
        len(list(
          sample_records[run_column].\
            drop_duplicates().\
            values))
      fastq_file_name = \
        list(sample_records[file_column].values)[0]
      file_name_pattern = \
        re.compile(r'(\S+)_S\d+_L00\d_(R|I)(\d)_001\.fastq\.gz')
      sample_prefix_match = \
        re.match(
          file_name_pattern,
          os.path.basename(fastq_file_name))
      if sample_prefix_match is None:
        raise ValueError(
                'Failed to match fastq file for {0}'.\
                  format(fastq_file_name))
      sample_prefix = sample_prefix_match.groups()[0]
      sample_records = \
        sample_records[[run_column,'fastq_dir']].\
          drop_duplicates()
      sample_records = \
        sample_records.\
          to_dict(orient='records')
      formatted_run_records = dict()
      for i,run in enumerate(sample_records):
        run_igf_id = run.get(run_column)
        fastq_dir = run.get('fastq_dir')
        tmp_output_path = \
          os.path.join(tmp_dir,feature,sample_igf_id,run_igf_id)
        if not os.path.exists(tmp_output_path):
          os.makedirs(tmp_output_path)
        formatted_run_records.\
          update({
            str(i):{
              "run_igf_id":run_igf_id,
              "fastq_dir":fastq_dir,
              "output_path":tmp_output_path
            }})
      formatted_analysis_description.\
        update({
          feature:{
            'sample_igf_id':sample_igf_id,
            'sample_name':sample_prefix,
            'run_count':total_runs_for_sample,
            'runs':formatted_run_records
          }})
    return formatted_analysis_description
  except Exception as e:
    raise ValueError(e)


def _validate_analysis_description(
      analysis_description,feature_types,sample_column='sample_igf_id',
      feature_column='feature_type',reference_column='reference'):
  """
  An internal function for validating analysis description. 
  This function loads the data to a Pandas DataFrame and extracts a list of
  formatted feature list and an unique sample list. Also, it checks the paths
  mentioned in the reference column and looks for the presence of duplicate features

  :param analysis_description: A list of analysis description dictionary
  :param feature_types: A list of immuno profiling feature names
  :param sample_column: Sample column name, default 'sample_igf_id'
  :param feature_column: Feature type column name, default 'feature_type'
  :param reference_column: Reference column name, default 'reference'
  """
  try:
    messages = list()
    analysis_list = list()
    sample_id_list = list()
    if not isinstance(analysis_description,list):
      raise ValueError(
              'Expecting a list of analysis_description, got {0}'.\
                format(type(analysis_description)))
    if not isinstance(feature_types,list):
      raise ValueError(
              'Expecting a list for feature_types, got {0}'.\
                format(type(feature_types)))
    df = pd.DataFrame(analysis_description)
    for c in (sample_column,feature_column):
      if c not in df.columns:
        messages.\
          append('missing {0} in analysis_data'.format(c))
    if len(messages) > 0:
      raise KeyError('Missing key column: {0}'.format(messages))
    analysis_list = \
      list(
        df[feature_column].\
          dropna().\
          drop_duplicates().\
          values)
    analysis_list = \
      set(
        [f.replace(' ','_').lower()
          for f in analysis_list])
    sample_id_list = \
      list(
        df[sample_column].\
          dropna().\
          drop_duplicates().\
          values)
    for f,f_data in df.groupby(feature_column):
      f = f.replace(' ','_').lower()
      f_samples = list(f_data[sample_column].values)
      if f not in feature_types:
        messages.\
          append('feature_type {0} is not defined: {1}'.\
                   format(f,f_samples))
      if len(f_samples) > 1:
        messages.\
          append('feature {0} has {1} samples: {2}'.\
                   format(f,len(f_samples),','.join(f_samples)))
    if reference_column in df.columns:
      ref_msg = \
        ['reference {0} does not exists'.format(r)
          for r in list(df['reference'].dropna().values)
            if not os.path.exists(r)]
      if len(ref_msg) > 0:
        messages.\
          extend(ref_msg)
    return sample_id_list, analysis_list, messages
  except Exception as e:
    raise ValueError(e)