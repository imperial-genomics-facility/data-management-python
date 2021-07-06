import os, sys, logging, subprocess, re, fnmatch
from typing import IO
import pandas as pd
from copy import copy
from airflow.models import Variable
from igf_data.utils.fileutils import get_temp_dir
from igf_data.utils.fileutils import remove_dir
from igf_data.utils.fileutils import read_json_data
from igf_data.utils.fileutils import copy_remote_file
from igf_data.utils.fileutils import check_file_path
from igf_data.utils.fileutils import copy_local_file
from igf_data.utils.fileutils import get_date_stamp
from igf_data.utils.fileutils import get_datestamp_label
from igf_data.utils.dbutils import read_dbconf_json
from igf_airflow.logging.upload_log_msg import post_image_to_channels
from igf_data.utils.singularity_run_wrapper import execute_singuarity_cmd
from igf_data.utils.analysis_fastq_fetch_utils import get_fastq_and_run_for_samples
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
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.utils.igf_irods_client import IGF_irods_uploader
from igf_data.utils.jupyter_nbconvert_wrapper import Notebook_runner
from igf_airflow.logging.upload_log_msg import send_log_to_channels
from igf_data.utils.box_upload import upload_file_or_dir_to_box
from igf_data.utils.tools.samtools_utils import convert_bam_to_cram
from igf_data.utils.tools.picard_util import Picard_tools
from igf_data.utils.tools.samtools_utils import run_bam_idxstat
from igf_data.utils.tools.samtools_utils import run_bam_stats
from igf_data.utils.tools.samtools_utils import index_bam_or_cram
from igf_data.utils.tools.samtools_utils import run_sort_bam
from igf_data.utils.project_analysis_utils import Project_analysis
from igf_data.utils.project_status_utils import Project_status
from jinja2 import Template,Environment,FileSystemLoader,select_autoescape
from igf_airflow.logging.upload_log_msg import log_success,log_failure,log_sleep
from igf_data.utils.tools.cellranger.cellranger_count_utils import extract_cellranger_count_metrics_summary

## DEFAULTS
GENOME_FASTA_TYPE = 'GENOME_FASTA'
GENE_REFFLAT_TYPE = 'GENE_REFFLAT'
RIBOSOMAL_INTERVAL_TYPE = 'RIBOSOMAL_INTERVAL'
ANALYSIS_CRAM_TYPE = 'ANALYSIS_CRAM'
CELLRANGER_MULTI_TYPE = 'CELLRANGER_MULTI'
PATTERNED_FLOWCELL_LIST = ['HISEQ4000','NEXTSEQ']
BOX_DIR_PREFIX = 'SecondaryAnalysis'
SAMTOOLS_EXE = 'samtools'
PICARD_JAR = '/picard/picard.jar'
DATABASE_CONFIG_FILE = Variable.get('test_database_config_file', default_var=None)
SCANPY_SINGLE_SAMPLE_TEMPLATE= Variable.get('scanpy_single_sample_template', default_var=None)
SCANPY_NOTEBOOK_IMAGE = Variable.get('scanpy_notebook_image', default_var=None)
SCIRPY_SINGLE_SAMPLE_TEMPLATE = Variable.get('scirpy_single_sample_template', default_var=None)
SCIRPY_NOTEBOOK_IMAGE = Variable.get('scirpy_notebook_image', default_var=None)
SEURAT_SINGLE_SAMPLE_TEMPLATE = Variable.get('seurat_single_sample_template', default_var=None)
SEURAT_NOTEBOOK_IMAGE = Variable.get('seurat_notebook_image', default_var=None)
CUTADAPT_IMAGE = Variable.get('cutadapt_singularity_image', default_var=None)
MULTIQC_IMAGE = Variable.get('multiqc_singularity_image', default_var=None)
PICARD_IMAGE = Variable.get('picard_singularity_image', default_var=None)
SLACK_CONF = Variable.get('analysis_slack_conf', default_var=None)
MS_TEAMS_CONF = Variable.get('analysis_ms_teams_conf', default_var=None)
ASANA_CONF = Variable.get('asana_conf', default_var=None)
ASANA_PROJECT = Variable.get('asana_analysis_project', default_var=None)
BOX_USERNAME = Variable.get('box_username', default_var=None)
BOX_CONFIG_FILE = Variable.get('box_config_file', default_var=None)
FTP_HOSTNAME = Variable.get('ftp_hostname', default_var=None)
FTP_USERNAME = Variable.get('ftp_username', default_var=None)
FTP_PROJECT_PATH = Variable.get('ftp_project_path', default_var=None)
BASE_RESULT_DIR = Variable.get('base_result_dir', default_var=None)
ALL_CELL_MARKER_LIST = Variable.get('all_cell_marker_list', default_var=None)
SAMTOOLS_IMAGE = Variable.get('samtools_singularity_image', default_var=None)
MULTIQC_TEMPLATE_FILE = Variable.get('multiqc_template_file', default_var=None)
FEATURE_TYPE = Variable.get('tenx_single_cell_immune_profiling_feature_types', default_var={})#.split(','),deserialize_json=True
IRDOS_EXE_DIR = Variable.get('irods_exe_dir', default_var=None)
CELLRANGER_EXE = Variable.get('cellranger_exe', default_var=None)
CELLRANGER_JOB_TIMEOUT = Variable.get('cellranger_job_timeout', default_var=None)
VELOCYTO_EXE = 'velocyto'

## FUNCTION
def run_velocyto_func(**context):
  try:
    ti = context.get('ti')
    xcom_pull_task = \
      context['params'].get('xcom_pull_task')
    xcom_pull_files_key = \
      context['params'].get('xcom_pull_files_key')
    analysis_description_xcom_pull_task = \
      context['params'].get('analysis_description_xcom_pull_task')
    analysis_description_xcom_key = \
      context['params'].get('analysis_description_xcom_key')
    cell_sorted_bam_name = \
      context['params'].get('cellranger_bam_path', 'count/cellsorted_possorted_genome_bam.bam')
    threads = \
      context['params'].get('threads', 1)
    samtools_memory = \
      context['params'].get('samtools_memory', 6000)
    sys.exit()                                                                  # FIX ME
    cellranger_output_dir = \
      ti.xcom_pull(
        task_ids=xcom_pull_task,
        key=xcom_pull_files_key)
    cell_sorted_bam_path = \
      os.path.join(cellranger_output_dir, cell_sorted_bam_name)
    cellranger_count_dir = \
      os.path.join(cellranger_output_dir, 'count')
    cellranger_outs_dir = \
      os.path.join(cellranger_output_dir, 'outs')
    check_file_path(cell_sorted_bam_path)
    if not os.path.exists(cellranger_outs_dir):
      os.symlink(cellranger_count_dir, cellranger_outs_dir)                     # create link for velocyto input path
    analysis_description = \
      ti.xcom_pull(
        task_ids=analysis_description_xcom_pull_task,
        key=analysis_description_xcom_key)
    analysis_description = pd.DataFrame(analysis_description)
    analysis_description['feature_type'] = \
      analysis_description['feature_type'].\
        map(lambda x: x.lower().replace(' ','_').replace('-','_'))
    gex_samples = \
      analysis_description[analysis_description['feature_type']=='gene_expression']\
        [['sample_igf_id','genome_build']]
    if len(gex_samples.index) == 0:
      raise ValueError('No gene expression entry found in analysis description')
    sample_igf_id = gex_samples['sample_igf_id'].values[0]
    dbparams = \
      read_dbconf_json(DATABASE_CONFIG_FILE)
    sa = SampleAdaptor(**dbparams)
    sa.start_session()
    genome_build = \
      sa.fetch_sample_species_name(
        sample_igf_id=sample_igf_id)
    sa.close_session()
    ## need to add cellranger ref and repeat mask gtf
    ref_genome = \
      Reference_genome_utils(
        genome_tag=genome_build,
        dbsession_class=sa.get_session_class())
    ref_genome.get_re()
    mask_file = \
      ref_genome._fetch_collection_files(
        collection_type=None,
        check_missing=True)
    gtf_file = \
      ref_genome._fetch_collection_files(
        collection_type=None,
        check_missing=True)
    commandline = [
      VELOCYTO_EXE,
      'run10x',
      '--mask={0}'.format(mask_file),
      '--samtools-memory={0}'.format(samtools_memory),
      '--samtools-threads={0}'.format(threads),
      cellranger_output_dir,
      gtf_file]
    commandline = ' '.join(commandline)
    container_tmp_dir = \
      get_temp_dir(use_ephemeral_space=True)
    bind_dir_lists = [
      '{0}:/tmp'.format(container_tmp_dir),
      cellranger_output_dir,
      os.path.dirname(mask_file),
      os.path.dirname(gtf_file)]
    execute_singuarity_cmd(
      image_path=SCANPY_NOTEBOOK_IMAGE,
      command_string=commandline,
      options=['--no-home','-C'],
      bind_dir_list=bind_dir_lists)
    loom_output = \
      os.path.join(
        cellranger_output_dir,
        'outs',
        '{0}.loom'.format(os.path.basename(cellranger_output_dir)))
    check_file_path(loom_output)
  except Exception as e:
    logging.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise ValueError(e)


def generate_cell_sorted_bam_func(**context):
  try:
    ti = context.get('ti')
    xcom_pull_task = \
      context['params'].get('xcom_pull_task')
    xcom_pull_files_key = \
      context['params'].get('xcom_pull_files_key')
    cellranger_bam_path = \
      context['params'].get('cellranger_bam_path', 'count/possorted_genome_bam.bam')
    cellsorted_bam_path = \
      context['params'].get('cellranger_bam_path', 'count/cellsorted_possorted_genome_bam.bam')
    threads = \
      context['params'].get('threads', 1)
    samtools_mem = \
      context['params'].get('samtools_mem', 6000)
    cellranger_output_dir = \
      ti.xcom_pull(
        task_ids=xcom_pull_task,
        key=xcom_pull_files_key)
    input_bam_path = \
      os.path.join(cellranger_output_dir, cellranger_bam_path)
    output_bam_path = \
      os.path.join(cellranger_output_dir, cellsorted_bam_path)
    if os.path.exists(output_bam_path):
      raise IOError(
        'File {0} already present. Check and remove it before restarting the job'.\
          format(output_bam_path))
    run_sort_bam(
      samtools_exe=SAMTOOLS_EXE,
      singularity_image=SAMTOOLS_IMAGE,
      input_bam_path=input_bam_path,
      output_bam_path=output_bam_path,
      sort_by_name=False,
      use_ephemeral_space=1,
      threads=threads,
      force=False,
      dry_run=False,
      cram_out=False,
      index_output=True,
      sort_params=['-m {0}M'.format(samtools_mem), '-t CB'])
  except Exception as e:
    logging.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise ValueError(e)


def create_and_update_qc_pages(**context):
  try:
    collection_type_list = \
      context['params'].get('collection_type_list')
    attribute_collection_file_type = \
      context['params'].get('attribute_collection_file_type',[ANALYSIS_CRAM_TYPE,CELLRANGER_MULTI_TYPE])
    pipeline_seed_table = \
      context['params'].get('pipeline_seed_table','analysis')
    sample_id_label = \
      context['params'].get('sample_id_label','sample_igf_id')
    remote_analysis_dir = \
      context['params'].get('remote_analysis_dir',os.path.join(FTP_PROJECT_PATH,'analysis'))
    pipeline_finished_status = \
      context['params'].get('pipeline_finished_status','FINISHED')
    use_ephemeral_space = \
      context['params'].get('use_ephemeral_space',False)
    analysis_data_json = \
      context['params'].get('analysis_data_json','analysis_data.json')
    chart_data_json = \
      context['params'].get('chart_data_json','analysis_chart_data.json')
    chart_data_csv = \
      context['params'].get('chart_data_csv','analysis_chart_data.csv')
    status_data_json = \
      context['params'].get('status_data_json','status_data.json')
    demultiplexing_pipeline_name = \
      context['params'].get('demultiplexing_pipeline_name','DemultiplexIlluminaFastq')
    dbparams = \
      read_dbconf_json(DATABASE_CONFIG_FILE)
    dag_run = context.get('dag_run')
    if dag_run is None or \
       dag_run.conf is None or \
       dag_run.conf.get('analysis_id') is None:
      raise ValueError('No analysis id found for collection')
    analysis_id = \
      dag_run.conf.get('analysis_id')
    analysis_type = \
        dag_run.conf.get('analysis_type')                                       # passing pipeline name as analysis_type
    aa = \
      AnalysisAdaptor(**dbparams)
    aa.start_session()
    project_igf_id = \
      aa.fetch_project_igf_id_for_analysis_id(
        analysis_id=int(analysis_id))                                           # fetch project id for analysis
    aa.close_session()
    (temp_status_output,analysis_data_output_file,
     chart_json_output_file,csv_output_file) = \
      _generate_status_and_analysis_page_data(
        project_igf_id=project_igf_id,
        db_session_class=aa.get_session_class(),
        collection_type_list=collection_type_list,
        pipeline_name=analysis_type,
        attribute_collection_file_type=attribute_collection_file_type,
        pipeline_seed_table=pipeline_seed_table,
        sample_id_label=sample_id_label,
        remote_analysis_dir=remote_analysis_dir,
        pipeline_finished_status=pipeline_finished_status,
        use_ephemeral_space=use_ephemeral_space,
        analysis_data_json=analysis_data_json,
        chart_data_json=chart_data_json,
        chart_data_csv=chart_data_csv,
        status_data_json=status_data_json,
        demultiplexing_pipeline_name=demultiplexing_pipeline_name)              # get json files
    remote_project_dir = \
      os.path.join(
        FTP_PROJECT_PATH,
        project_igf_id)
    remote_status_json_path = \
      os.path.join(
        remote_project_dir,
        status_data_json)
    remote_analysis_json_path = \
      os.path.join(
        remote_project_dir,
        analysis_data_json)
    remote_chart_file_path = \
      os.path.join(
        remote_project_dir,
        chart_data_json)
    remote_csv_file_path = \
      os.path.join(
        remote_project_dir,
        chart_data_csv)
    _check_and_copy_remote_file(
      remote_user=FTP_USERNAME,
      remote_host=FTP_HOSTNAME,
      source_file=temp_status_output,
      remote_file=remote_status_json_path)                                      # copy json data for status page
    _check_and_copy_remote_file(
      remote_user=FTP_USERNAME,
      remote_host=FTP_HOSTNAME,
      source_file=analysis_data_output_file,
      remote_file=remote_analysis_json_path)                                    # copy json data for analysis page
    _check_and_copy_remote_file(
      remote_user=FTP_USERNAME,
      remote_host=FTP_HOSTNAME,
      source_file=chart_json_output_file,
      remote_file=remote_chart_file_path)                                       # copy json data for analysis charts
    _check_and_copy_remote_file(
      remote_user=FTP_USERNAME,
      remote_host=FTP_HOSTNAME,
      source_file=csv_output_file,
      remote_file=remote_csv_file_path)                                         # copy json data for analysis csv data
  except Exception as e:
    logging.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise ValueError(e)


def _check_and_copy_remote_file(
      remote_user,remote_host,source_file,remote_file):
    '''
    An internal static method for copying files to remote path

    :param remote_user: Username for the remote server
    :param remote_host: Hostname for the remote server
    :param source_file: Source filepath
    :param remote_file: Remote filepath
    '''
    try:
      check_file_path(source_file)
      remote_config = '{0}@{1}'.format(remote_user,remote_host)
      os.chmod(
        source_file,
        mode=0o754)
      copy_remote_file(
        source_path=source_file,
        destinationa_path=remote_file,
        destination_address=remote_config,
        force_update=True)                                                      # create dir and copy file to remote
    except Exception as e:
      raise ValueError(
              'Failed to copy remote file {0}, error: {1}'.\
                format(remote_file,e))


def _generate_status_and_analysis_page_data(
      project_igf_id,db_session_class,collection_type_list,pipeline_name,
      attribute_collection_file_type,pipeline_seed_table,sample_id_label,remote_analysis_dir,
      pipeline_finished_status='FINISHED',use_ephemeral_space=False,analysis_data_json='analysis_data.json',
      chart_data_json='analysis_chart_data.json',chart_data_csv='analysis_chart_data.csv',
      status_data_json='status_data.json',demultiplexing_pipeline_name='DemultiplexIlluminaFastq'):
  """
  An internal function for gviz json data generation for status and analysis page

  :param project_igf_id: Project igf id
  :param db_session_class: SQLAlchemy session class for db connection
  :param collection_type_list: A list of collection types for db lookup
  :param pipeline_name: Pipeline name for db lookup
  :param attribute_collection_file_type: Attribute collection type for csv data
  :param pipeline_seed_table: Pipeline seed table
  :param sample_id_label: Sample igf id label, default sample_igf_id
  :param remote_analysis_dir: FTP analysis dir path
  :param pipeline_finished_status: Pipeline finished status, default FINISHED
  :param use_ephemeral_space: A toggle for using ephemeral temp dir, default False
  :param analysis_data_json: Name of the analysis page json data, default analysis_data.json
  :param chart_data_json: Name of analysis chart json data default analysis_chart_data.json
  :param chart_data_csv: Name of analysis csv data, default analysis_chart_data.csv
  :param status_data_json: Name of status json data, default status_data.json
  :param demultiplexing_pipeline_name: Name of demultiplexing pipeline name, default igf_demultiplexing
  :returns: A list of json data file paths
              * temp_status_output
              * analysis_data_output_file
              * chart_json_output_file
              * csv_output_file
  """
  try:
    temp_dir = get_temp_dir(use_ephemeral_space=use_ephemeral_space)
    analysis_data_output_file = os.path.join(temp_dir,analysis_data_json)
    chart_json_output_file = os.path.join(temp_dir,chart_data_json)
    csv_output_file = os.path.join(temp_dir,chart_data_csv)
    temp_status_output = os.path.join(temp_dir,status_data_json)
    ps = \
      Project_status(
        igf_session_class=db_session_class,
        project_igf_id=project_igf_id)
    ps.generate_gviz_json_file(
      output_file=temp_status_output,
      demultiplexing_pipeline=demultiplexing_pipeline_name,
      analysis_pipeline=pipeline_name)
    prj_data = \
      Project_analysis(
        igf_session_class=db_session_class,
        collection_type_list=collection_type_list,
        remote_analysis_dir=remote_analysis_dir,
        attribute_collection_file_type=attribute_collection_file_type,
        pipeline_name=pipeline_name,
        pipeline_seed_table=pipeline_seed_table,
        pipeline_finished_status=pipeline_finished_status,
        use_ephemeral_space=use_ephemeral_space,
        sample_id_label=sample_id_label)
    prj_data.\
      get_analysis_data_for_project(
        project_igf_id=project_igf_id,
        output_file=analysis_data_output_file,
        chart_json_output_file=chart_json_output_file,
        csv_output_file=csv_output_file)
    check_file_path(temp_status_output)
    check_file_path(analysis_data_output_file)
    check_file_path(chart_json_output_file)
    check_file_path(csv_output_file)
    return temp_status_output,analysis_data_output_file,chart_json_output_file,csv_output_file
  except Exception as e:
    raise ValueError(
            'Failed to generate data for analysis page, error: {0}'.\
              format(e))


def clean_up_files(**context):
  try:
    ti = context.get('ti')
    xcom_pull_task = \
      context['params'].get('xcom_pull_task')
    xcom_pull_files_key = \
      context['params'].get('xcom_pull_files_key')
    file_list = \
      ti.xcom_pull(
        task_ids=xcom_pull_task,
        key=xcom_pull_files_key)
    if isinstance(file_list,list):
      for path in file_list:
        if os.path.isdir(path):
          raise ValueError(
                  'Its not safe to delete dirs automatically, {0}'.\
                    format(path))
        os.remove(path)
    elif isinstance(file_list,str):
      if os.path.isdir(file_list):
        raise ValueError(
                'Its not safe to delete dirs automatically, {0}'.\
                  format(file_list))
      os.remove(file_list)
    else:
      raise TypeError(
              'Expecting a list of a filepath for cleanup, got {0}'.\
                format(type(file_list)))
  except Exception as e:
    logging.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise ValueError(e)


def change_pipeline_status(**context):
  try:
    dag_run = context.get('dag_run')
    new_status = \
      context['params'].get('new_status')
    no_change_status = \
      context['params'].get('no_change_status')
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('analysis_description') is not None:
      analysis_id = \
        dag_run.conf.get('analysis_id')
      analysis_type = \
        dag_run.conf.get('analysis_type')
      status = \
        _check_and_mark_analysis_seed(
          analysis_id=analysis_id,
          anslysis_type=analysis_type,
          new_status=new_status,
          no_change_status=no_change_status,
          database_config_file=DATABASE_CONFIG_FILE)
      if not status:
        raise ValueError(
                'Failed to update pipeline seed for analysis id {0} and type {1}'.\
                  format(analysis_id,analysis_type))
  except Exception as e:
    logging.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise ValueError(e)


def index_and_copy_bam_for_parallel_analysis(**context):
  try:
    ti = context.get('ti')
    list_of_tasks = \
      context['params'].get('list_of_tasks')
    xcom_pull_task = \
      context['params'].get('xcom_pull_task')
    xcom_pull_files_key = \
      context['params'].get('xcom_pull_files_key')
    cellranger_bam_path = \
      context['params'].get('cellranger_bam_path','count/possorted_genome_bam.bam')
    cellranger_output_dir = \
      ti.xcom_pull(
        task_ids=xcom_pull_task,
        key=xcom_pull_files_key)
    bam_file = \
      os.path.join(cellranger_output_dir,cellranger_bam_path)
    output_temp_bams = \
      _check_bam_index_and_copy(
      samtools_exe=SAMTOOLS_EXE,
      singularity_image=SAMTOOLS_IMAGE,
      bam_file=bam_file,
      list_of_analysis=list_of_tasks)
    for analysis_name,bam_file in output_temp_bams.items():                     # push temp bam paths to xcom
      ti.xcom_push(
        key=analysis_name,
        value=bam_file)
    return list_of_tasks
  except Exception as e:
    logging.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise ValueError(e)


def _check_bam_index_and_copy(
      samtools_exe,singularity_image,bam_file,list_of_analysis,
      use_ephemeral_space=True,threads=1,dry_run=False):
  """
  An internal function for checking bam index and copying one file per analysis

  :param samtools_exe: Samtools exe path
  :param singularity_image: Singularity image path
  :param bam_file: Input bam file path for copy
  :param list_of_analysis: A list of analysis name for bam copy
  :param threads: Number of threads for samtools index, default 1
  :param dry_run: A toggle for dry_run, default False
  :param use_ephemeral_space: A toggle for using ephemeral space on hpc, default True
  :return: A dictionary, keys are the analysis names and values are the temp bam paths
  """
  try:
    output_temp_dirs = dict()
    check_file_path(bam_file)
    if not isinstance(list_of_analysis,list) or \
       len(list_of_analysis)==0:
      raise TypeError('Expecing a list of analysis names for copy bam')
    bai_file = '{0}.bai'.format(bam_file)                                       # check bam index path
    if not os.path.exists(bai_file):                                            # create index in not found
      _ = \
        index_bam_or_cram(
          samtools_exe=samtools_exe,
          input_path=bam_file,
          threads=threads,
          singuarity_image=singularity_image,
          dry_run=dry_run)
    if not dry_run:
      check_file_path(bai_file)                                                 # check final bam index
    for analysis_name in list_of_analysis:
      temp_dir = \
        get_temp_dir(use_ephemeral_space=use_ephemeral_space)
      temp_bam = \
        os.path.join(temp_dir,os.path.basename(bam_file))
      temp_bai = \
        os.path.join(temp_dir,os.path.basename(bai_file))
      copy_local_file(
        bam_file,temp_bam)
      if not dry_run:
        copy_local_file(
          bai_file,temp_bai)
      output_temp_dirs.\
        update({analysis_name:temp_bam})
    if len(output_temp_dirs.keys())==0:
      raise ValueError(
              'No output temp bam path found for input {0}'\
                .format(bam_file))
    return output_temp_dirs
  except Exception as e:
    raise ValueError(
            'Failed to copy bam and index, error: {0}'.format(e))


def run_multiqc_for_cellranger(**context):
  try:
    ti = context.get('ti')
    list_of_analysis_xcoms_and_tasks = \
      context['params'].get('list_of_analysis_xcoms_and_tasks')
    analysis_description_xcom_pull_task = \
      context['params'].get('analysis_description_xcom_pull_task')
    analysis_description_xcom_key = \
      context['params'].get('analysis_description_xcom_key')
    multiqc_html_file_xcom_key = \
      context['params'].get('multiqc_html_file_xcom_key')
    multiqc_data_file_xcom_key = \
      context['params'].get('multiqc_data_file_xcom_key')
    use_ephemeral_space = \
      context['params'].get('use_ephemeral_space',True)
    tool_order_list = \
      context['params'].get('tool_order_list',['fastp','picard','samtools'])
    multiqc_options = \
      context['params'].get('multiqc_options',['--zip-data-dir'])
    multiqc_exe = \
      context['params'].get('multiqc_exe','multiqc')
    temp_work_dir = \
      get_temp_dir(use_ephemeral_space=use_ephemeral_space)
    ### fetch sample id and genome build from analysis description
    analysis_description = \
      ti.xcom_pull(
        task_ids=analysis_description_xcom_pull_task,
        key=analysis_description_xcom_key)
    analysis_description = pd.DataFrame(analysis_description)
    analysis_description['feature_type'] = \
      analysis_description['feature_type'].\
        map(lambda x: x.lower().replace(' ','_').replace('-','_'))
    gex_samples = \
      analysis_description[analysis_description['feature_type']=='gene_expression']\
        [['sample_igf_id','genome_build']]
    if len(gex_samples.index) == 0:
      raise ValueError('No gene expression entry found in analysis description')
    genome_build = gex_samples['genome_build'].values[0]
    sample_igf_id = gex_samples['sample_igf_id'].values[0]
    ### fetch project id from analysis entry
    dbparams = \
      read_dbconf_json(DATABASE_CONFIG_FILE)
    dag_run = context.get('dag_run')
    if dag_run is None or \
       dag_run.conf is None or \
       dag_run.conf.get('analysis_id') is None:
      raise ValueError('No analysis id found for collection')
    analysis_id = \
      dag_run.conf.get('analysis_id')
    aa = \
      AnalysisAdaptor(**dbparams)
    aa.start_session()
    project_igf_id = \
      aa.fetch_project_igf_id_for_analysis_id(
        analysis_id=int(analysis_id))
    aa.close_session()
    ### fetch analysis file paths
    analysis_paths_list = list()
    if not isinstance(list_of_analysis_xcoms_and_tasks,dict) or \
       len(list_of_analysis_xcoms_and_tasks.keys())==0:
      raise TypeError(
              'No analysis ids found for xcom fetching, {0}'.\
              format(list_of_analysis_xcoms_and_tasks))
    for xcom_pull_task,key_list in list_of_analysis_xcoms_and_tasks.items():
      if isinstance(key_list,list):
        for xcom_pull_files_key in key_list:
          file_paths = \
            ti.xcom_pull(
              task_ids=xcom_pull_task,
              key=xcom_pull_files_key)
          if isinstance(file_paths,list):
            analysis_paths_list.\
              extend(file_paths)
          elif isinstance(file_paths,str):
            analysis_paths_list.\
              append(file_paths)
          else:
            raise TypeError(
                    'Expecting a list or string of file paths, got: {0}, task: {1}, key: {2}'.\
                    format(type(file_paths),xcom_pull_task,xcom_pull_files_key))
      elif isinstance(key_list,str):
        file_paths = \
          ti.xcom_pull(
            task_ids=xcom_pull_task,
            key=key_list)
        if isinstance(file_paths,list):
          analysis_paths_list.\
            extend(file_paths)
        elif isinstance(file_paths,str):
          analysis_paths_list.\
            append(file_paths)
        else:
          raise TypeError(
                  'Expecting a list or string of file paths, got: {0}, task: {1}, key: {2}'.\
                  format(type(file_paths),xcom_pull_task,key_list))
      else:
        raise TypeError(
                'Expecting a list of string for xcom pull keys, got {0}, keys: {1}'.\
                  format(type(key_list),key_list))
    ### configure and run Multiqc
    multiqc_html,multiqc_data,cmd = \
      _configure_and_run_multiqc(
        analysis_paths_list=analysis_paths_list,
        project_igf_id=project_igf_id,
        sample_igf_id=sample_igf_id,
        work_dir=temp_work_dir,
        genome_build=genome_build,
        multiqc_template_file=MULTIQC_TEMPLATE_FILE,
        tool_order_list=tool_order_list,
        singularity_mutiqc_image=MULTIQC_IMAGE,
        multiqc_params=multiqc_options,
        multiqc_exe=multiqc_exe,
        dry_run=False)
    ti.xcom_push(
      key=multiqc_html_file_xcom_key,
      value=multiqc_html)
    ti.xcom_push(
      key=multiqc_data_file_xcom_key,
      value=multiqc_data)
  except Exception as e:
    logging.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise ValueError(e)



def _configure_and_run_multiqc(
      analysis_paths_list,project_igf_id,sample_igf_id,work_dir,
      genome_build,multiqc_template_file,singularity_mutiqc_image,
      tool_order_list,multiqc_params,multiqc_exe='muliqc',dry_run=False):
  """
  An internal function for configuribg and executing MultiQC for single cell data
  :param analysis_paths_list: A list of analysis output to run Multiqc
  :param project_igf_id: Project igf id
  :param sample_igf_id: Sample igf id
  :param work_dir: Path to write temp output files, must exists
  :param tool_order_list: Tool order list for MultiQC
  :param singularity_mutiqc_image: Singularity image path for MultiQC
  :param multiqc_params: A list of params to multiqc
  :param multiqc_exe: Multiqc exe path, default multiqc
  :param dry_run: Toggle for dry run, default False
  :returns: MultiQC html path, MultiQC data path, singularity command
  """
  try:
    ### final check
    if len(analysis_paths_list)== 0:
      raise ValueError('No analysis file found for multiqc report')
    ### write a multiqc input file
    multiqc_input_file = \
      os.path.join(work_dir,'multiqc.txt')
    with open(multiqc_input_file,'w') as fp:
      for file_path in analysis_paths_list:
        check_file_path(file_path)
        fp.write('{}\n'.format(file_path))
    date_stamp = get_date_stamp()
    #
    # write multiqc config file
    #
    check_file_path(multiqc_template_file)
    multiqc_conf_file = \
      os.path.join(
        work_dir,os.path.basename(multiqc_template_file))
    template_env = \
      Environment(
        loader=\
          FileSystemLoader(
            searchpath=os.path.dirname(multiqc_template_file)),
        autoescape=select_autoescape(['html', 'xml']))
    multiqc_conf = \
      template_env.\
        get_template(
          os.path.basename(multiqc_template_file))
    multiqc_conf.\
      stream(
        project_igf_id=project_igf_id,
        sample_igf_id=sample_igf_id,
        tag_name='Single cell gene expression - {0}'.format(genome_build),
        date_stamp=date_stamp,
        tool_order_list=tool_order_list).\
      dump(multiqc_conf_file)
    #
    # configure multiqc run
    #
    multiqc_report_title = \
      'Project:{0},Sample:{1}'.\
        format(project_igf_id,sample_igf_id)
    multiqc_cmd = [
      multiqc_exe,
      '--file-list',multiqc_input_file,
      '--outdir',work_dir,
      '--title',multiqc_report_title,
      '-c',multiqc_conf_file]                                                   # multiqc base parameter
    if not isinstance(multiqc_params,list):
      raise TypeError(
              'Expecting a list of params for multiqc run, got: {0}'.\
                format(type(multiqc_params)))
    multiqc_cmd.\
      extend(multiqc_params)
    #
    # configure singularity run
    #
    bind_dir_list = \
      [os.path.dirname(path)
        for path in analysis_paths_list]
    bind_dir_list.append(work_dir)
    bind_dir_list = list(set(bind_dir_list))
    cmd = \
      execute_singuarity_cmd(
        image_path=singularity_mutiqc_image,
        command_string=' '.join(multiqc_cmd),
        bind_dir_list=bind_dir_list,
        dry_run=dry_run)
    if dry_run:
      return None,None,cmd
    else:
      multiqc_html = None
      multiqc_data = None
      for root, _,files in os.walk(top=work_dir):
        for file in files:
          if fnmatch.fnmatch(file, '*.html'):
            multiqc_html = os.path.join(root,file)
          if fnmatch.fnmatch(file, '*.zip'):
            multiqc_data = os.path.join(root,file)
      if multiqc_html is None or \
         multiqc_data is None:
        raise IOError('Failed to get Multiqc output file')
      check_file_path(multiqc_html)
      check_file_path(multiqc_data)
      return multiqc_html,multiqc_data,cmd
  except Exception as e:
    raise ValueError(
            'Failed to configure and run multiqc, error: {0}'.\
              format(e))



def run_samtools_for_cellranger(**context):
  try:
    ti = context.get('ti')
    xcom_pull_task = \
      context['params'].get('xcom_pull_task')
    xcom_pull_files_key = \
      context['params'].get('xcom_pull_files_key')
    analysis_description_xcom_pull_task = \
      context['params'].get('analysis_description_xcom_pull_task')
    analysis_description_xcom_key = \
      context['params'].get('analysis_description_xcom_key')
    use_ephemeral_space = \
      context['params'].get('use_ephemeral_space',True)
    load_metrics_to_cram = \
      context['params'].get('load_metrics_to_cram',False)
    threads = \
      context['params'].get('threads',1)
    samtools_command = \
      context['params'].get('samtools_command')
    analysis_files_xcom_key = \
      context['params'].get('analysis_files_xcom_key')
    temp_output_dir = \
      get_temp_dir(use_ephemeral_space=use_ephemeral_space)
    analysis_description = \
      ti.xcom_pull(
        task_ids=analysis_description_xcom_pull_task,
        key=analysis_description_xcom_key)
    analysis_description = pd.DataFrame(analysis_description)
    analysis_description['feature_type'] = \
      analysis_description['feature_type'].\
        map(lambda x: x.lower().replace(' ','_').replace('-','_'))
    gex_samples = \
      analysis_description[analysis_description['feature_type']=='gene_expression']\
        [['sample_igf_id','genome_build']]
    if len(gex_samples.index) == 0:
      raise ValueError('No gene expression entry found in analysis description')
    sample_igf_id = gex_samples['sample_igf_id'].values[0]
    bam_file = \
      ti.xcom_pull(
        task_ids=xcom_pull_task,
        key=xcom_pull_files_key)
    if isinstance(bam_file,list):
      bam_file = bam_file[0]
    temp_output = None
    if samtools_command == 'idxstats':
      temp_output,_ = \
        run_bam_idxstat(
          samtools_exe=SAMTOOLS_EXE,
          bam_file=bam_file,
          output_dir=temp_output_dir,
          output_prefix=sample_igf_id,
          singuarity_image=SAMTOOLS_IMAGE,
          force=True)
    elif samtools_command == 'stats':
      temp_output,_,stats_metrics = \
        run_bam_stats(
          samtools_exe=SAMTOOLS_EXE,
          bam_file=bam_file,
          output_dir=temp_output_dir,
          output_prefix=sample_igf_id,
          threads=threads,
          singuarity_image=SAMTOOLS_IMAGE,
          force=True)
      if load_metrics_to_cram and \
           len(stats_metrics) > 0:
        dbparams = \
        read_dbconf_json(DATABASE_CONFIG_FILE)
        ca = CollectionAdaptor(**dbparams)
        attribute_data = \
          ca.prepare_data_for_collection_attribute(
            collection_name=sample_igf_id,
            collection_type=ANALYSIS_CRAM_TYPE,
            data_list=stats_metrics)
        ca.start_session()
        try:
          ca.create_or_update_collection_attributes(
            data=attribute_data,
            autosave=False)
          ca.commit_session()
          ca.close_session()
        except Exception as e:
          ca.rollback_session()
          ca.close_session()
          raise ValueError(
                  'Failed to load data to db: {0}'.\
                  format(e))
    else:
      raise ValueError(
              'Samtools command {0} not supported'.\
              format(samtools_command))
    if temp_output is not None:
      ti.xcom_push(
        key=analysis_files_xcom_key,
        value=temp_output)
  except Exception as e:
    logging.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise ValueError(e)


def run_picard_for_cellranger(**context):
  try:
    ti = context.get('ti')
    xcom_pull_task = \
      context['params'].get('xcom_pull_task')
    xcom_pull_files_key = \
      context['params'].get('xcom_pull_files_key')
    analysis_description_xcom_pull_task = \
      context['params'].get('analysis_description_xcom_pull_task')
    analysis_description_xcom_key = \
      context['params'].get('analysis_description_xcom_key')
    use_ephemeral_space = \
      context['params'].get('use_ephemeral_space')
    load_metrics_to_cram = \
      context['params'].get('load_metrics_to_cram')
    java_param = \
      context['params'].get('java_param')
    picard_command = \
      context['params'].get( 'picard_command')
    picard_option = \
      context['params'].get('picard_option')
    analysis_files_xcom_key = \
      context['params'].get('analysis_files_xcom_key')
    bam_files_xcom_key = \
      context['params'].get('bam_files_xcom_key')
    analysis_description = \
      ti.xcom_pull(
        task_ids=analysis_description_xcom_pull_task,
        key=analysis_description_xcom_key)
    analysis_description = pd.DataFrame(analysis_description)
    analysis_description['feature_type'] = \
      analysis_description['feature_type'].\
        map(lambda x: x.lower().replace(' ','_').replace('-','_'))
    gex_samples = \
      analysis_description[analysis_description['feature_type']=='gene_expression']\
        [['sample_igf_id','genome_build']]
    if len(gex_samples.index) == 0:
      raise ValueError('No gene expression entry found in analysis description')
    #genome_build = gex_samples['genome_build'].values[0]
    sample_igf_id = gex_samples['sample_igf_id'].values[0]
    dbparams = \
      read_dbconf_json(DATABASE_CONFIG_FILE)
    sa = SampleAdaptor(**dbparams)
    sa.start_session()
    genome_build = \
      sa.fetch_sample_species_name(
        sample_igf_id=sample_igf_id)
    sa.close_session()
    bam_file = \
      ti.xcom_pull(
        task_ids=xcom_pull_task,
        key=xcom_pull_files_key)
    if isinstance(bam_file,list):
      bam_file = bam_file[0]
    ref_genome = \
      Reference_genome_utils(
        genome_tag=genome_build,
        dbsession_class=sa.get_session_class(),
        genome_fasta_type=GENOME_FASTA_TYPE,
        gene_reflat_type=GENE_REFFLAT_TYPE,
        ribosomal_interval_type=RIBOSOMAL_INTERVAL_TYPE)
    genome_fasta = ref_genome.get_genome_fasta()
    ref_flat_file = ref_genome.get_gene_reflat()
    ribosomal_interval_file = ref_genome.get_ribosomal_interval()
    sa.start_session()
    sample_platform_records = \
      sa.fetch_seqrun_and_platform_list_for_sample_id(
        sample_igf_id=sample_igf_id)
    sa.close_session()
    sample_platform_records.\
      drop_duplicates(inplace=True)
    if len(sample_platform_records.index)==0:
      raise ValueError(
              'Failed to get platform information for sample {0}'.\
                format(sample_igf_id))
    platform_name = \
      sample_platform_records['model_name'].values[0]
    patterned_flowcell = False
    if platform_name in PATTERNED_FLOWCELL_LIST:                              # check for patterned flowcell
      patterned_flowcell = True
    temp_output_dir = \
      get_temp_dir(use_ephemeral_space=True)
    picard = \
       Picard_tools(
        java_exe='java',
        java_param=java_param,
        singularity_image=PICARD_IMAGE,
        picard_jar=PICARD_JAR,
        input_files=[bam_file],
        output_dir=temp_output_dir,
        ref_fasta=genome_fasta,
        patterned_flowcell=patterned_flowcell,
        ref_flat_file=ref_flat_file,
        picard_option=picard_option,
        output_prefix=sample_igf_id,
        use_ephemeral_space=use_ephemeral_space,
        ribisomal_interval=ribosomal_interval_file)
    temp_output_files,picard_command_line,picard_metrics = \
      picard.run_picard_command(
        command_name=picard_command)
    output_analysis_files = list()
    output_bam_files = list()
    for file in temp_output_files:
      if file.endswith('.bam'):
        output_bam_files.append(file)
      else:
        output_analysis_files.append(file)
    if load_metrics_to_cram and \
       len(picard_metrics) > 0:
      ca = CollectionAdaptor(**dbparams)
      attribute_data = \
        ca.prepare_data_for_collection_attribute(
          collection_name=sample_igf_id,
          collection_type=ANALYSIS_CRAM_TYPE,
          data_list=picard_metrics)
      ca.start_session()
      try:
        ca.create_or_update_collection_attributes(
          data=attribute_data,
          autosave=False)                                                     # load data to collection attribute table
        ca.commit_session()
        ca.close_session()
      except Exception as e:
        ca.rollback_session()
        ca.close_session()
        raise ValueError(
                'Failed to load pcard matrics to cram, error: {0}'.\
                 format(e))
    if len(output_analysis_files) > 0:
      ti.xcom_push(
        key=analysis_files_xcom_key,
        value=output_analysis_files)
    if len(output_bam_files) > 0:
      ti.xcom_push(
        key=bam_files_xcom_key,
        value=output_bam_files)
    message = \
      'Finished Picard {0}, sample: {1}, command: {2}'.\
        format(picard_command, sample_igf_id, picard_command_line)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='pass')
  except Exception as e:
    logging.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise ValueError(e)


def convert_bam_to_cram_func(**context):
  try:
    ti = context.get('ti')
    xcom_pull_task = \
      context['params'].get('xcom_pull_task')
    xcom_pull_files_key = \
      context['params'].get('xcom_pull_files_key')
    analysis_description_xcom_pull_task = \
      context['params'].get('analysis_description_xcom_pull_task')
    analysis_description_xcom_key = \
      context['params'].get('analysis_description_xcom_key')
    use_ephemeral_space = \
      context['params'].get('use_ephemeral_space')
    threads = \
      context['params'].get('threads')
    analysis_name = \
      context['params'].get('analysis_name')
    collection_table = \
      context['params'].get('collection_table')
    cram_files_xcom_key = \
      context['params'].get('cram_files_xcom_key')
    cellranger_bam_path = \
      context['params'].get('cellranger_bam_path','count/possorted_genome_bam.bam')
    analysis_description = \
      ti.xcom_pull(
        task_ids=analysis_description_xcom_pull_task,
        key=analysis_description_xcom_key)
    analysis_description = pd.DataFrame(analysis_description)
    analysis_description['feature_type'] = \
      analysis_description['feature_type'].\
        map(lambda x: x.lower().replace(' ','_').replace('-','_'))
    gex_samples = \
      analysis_description[analysis_description['feature_type']=='gene_expression']\
        [['sample_igf_id','genome_build']]
    if len(gex_samples.index) == 0:
      raise ValueError('No gene expression entry found in analysis description')
    sample_igf_id = gex_samples['sample_igf_id'].values[0]
    #genome_build = gex_samples['genome_build'].values[0]
    dbparams = \
      read_dbconf_json(DATABASE_CONFIG_FILE)
    sa = SampleAdaptor(**dbparams)
    sa.start_session()
    genome_build = \
      sa.fetch_sample_species_name(
        sample_igf_id=sample_igf_id)
    sa.close_session()
    cellranger_output_dir = \
      ti.xcom_pull(
        task_ids=xcom_pull_task,
        key=xcom_pull_files_key)
    bam_file = \
      os.path.join(cellranger_output_dir,cellranger_bam_path)
    if isinstance(bam_file,list):
      bam_file = bam_file[0]
    dbparams = \
      read_dbconf_json(DATABASE_CONFIG_FILE)
    aa = \
      AnalysisAdaptor(**dbparams)
    ref_genome = \
        Reference_genome_utils(\
          genome_tag=genome_build,
          dbsession_class=aa.get_session_class(),
          genome_fasta_type=GENOME_FASTA_TYPE)
    genome_fasta = ref_genome.get_genome_fasta()
    temp_work_dir = get_temp_dir(use_ephemeral_space=use_ephemeral_space)
    cram_file = os.path.basename(bam_file).replace('.bam','.cram')
    cram_file = os.path.join(temp_work_dir,cram_file)
    convert_bam_to_cram(
      samtools_exe=SAMTOOLS_EXE,
      bam_file=bam_file,
      reference_file=genome_fasta,
      cram_path=cram_file,
      use_ephemeral_space=True,
      singuarity_image=SAMTOOLS_IMAGE,
      threads=threads)
    au = \
      Analysis_collection_utils(
        dbsession_class=aa.get_session_class(),
        analysis_name=analysis_name,
        tag_name=genome_build,
        collection_name=sample_igf_id,
        collection_type=ANALYSIS_CRAM_TYPE,
        collection_table=collection_table,
        base_path=BASE_RESULT_DIR)
    output_cram_list = \
      au.load_file_to_disk_and_db(\
        input_file_list=[cram_file],
        file_suffix='cram',
        withdraw_exisitng_collection=True)
    final_output_list = list()
    for cram in output_cram_list:
      _ = \
        index_bam_or_cram(\
          samtools_exe=SAMTOOLS_EXE,
          input_path=cram,
          singuarity_image=SAMTOOLS_IMAGE,
          threads=threads)                                                    # index cram files
      final_output_list.append(cram)
      cram_index = '{0}.crai'.format(cram)                                    # cram index has suffix .crai
      check_file_path(cram_index)
      final_output_list.append(cram_index)
    ti.xcom_push(
      key=cram_files_xcom_key,
      value=final_output_list)
    message = \
      'Finished BAM to CRAM conversion for sample {0}: {1}'.\
        format(sample_igf_id, final_output_list)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='pass')
  except Exception as e:
    logging.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise ValueError(e)


def upload_analysis_file_to_box(**context):
  try:
    ti = context.get('ti')
    xcom_pull_task = \
      context['params'].get('xcom_pull_task')
    xcom_pull_files_key = \
      context['params'].get('xcom_pull_files_key')
    analysis_tag = \
      context['params'].get('analysis_tag')
    file_list_for_copy = \
      ti.xcom_pull(
        task_ids=xcom_pull_task,
        key=xcom_pull_files_key)
    if isinstance(file_list_for_copy,str):
      file_list_for_copy = [file_list_for_copy]
    dbparams = \
      read_dbconf_json(DATABASE_CONFIG_FILE)
    dag_run = context.get('dag_run')
    if dag_run is None or \
       dag_run.conf is None or \
       dag_run.conf.get('analysis_id') is None:
      raise ValueError('No analysis id found for collection')
    analysis_id = \
        dag_run.conf.get('analysis_id')
    aa = \
      AnalysisAdaptor(**dbparams)
    aa.start_session()
    project_igf_id = \
      aa.fetch_project_igf_id_for_analysis_id(
        analysis_id=int(analysis_id))
    analysis_record = \
      aa.fetch_analysis_records_analysis_id(
        analysis_id=int(analysis_id),
        output_mode='one_or_none')
    aa.close_session()
    if analysis_record is None:
      raise ValueError(
              'No analysis records found for analysis_id {0}'.\
              format(analysis_id))
    if analysis_record is None:
      raise KeyError('Missing required analysis record')
    analysis_name = analysis_record.analysis_name
    dag_id = context['task'].dag_id
    box_dir = \
      os.path.join(
        BOX_DIR_PREFIX,
        project_igf_id,
        dag_id,
        analysis_name,
        analysis_tag)
    for file_path in file_list_for_copy:
      upload_file_or_dir_to_box(
        box_config_file=BOX_CONFIG_FILE,
        file_path=file_path,
        upload_dir=box_dir,
        box_username=BOX_USERNAME,
        skip_existing=False)
  except Exception as e:
    logging.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise ValueError(e)


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
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
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
    tag_name = \
      context['params'].get('tag_name','no_tag')
    collection_as_sample = \
      context['params'].get('collection_as_sample',True)
    database_config_file = DATABASE_CONFIG_FILE
    base_result_dir = BASE_RESULT_DIR
    dbparams = \
      read_dbconf_json(database_config_file)
    base = \
      BaseAdaptor(**dbparams)
    collection_name = \
      ti.xcom_pull(
        task_ids=collection_name_task,
        key=collection_name_key)
    temp_file = \
      ti.xcom_pull(
        task_ids=file_name_task,
        key=file_name_key)
    if collection_as_sample:
      sa = SampleAdaptor(**dbparams)
      sa.start_session()
      tag_name = \
        sa.fetch_sample_species_name(
          sample_igf_id=collection_name)
      sa.close_session()
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
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
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
      context['params'].get('count_dir','count')
    vdj_dir = \
      context['params'].get('vdj_dir','vdj')
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
    s_genes = None
    g2m_genes = None
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
    if isinstance(analysis_description, list):
      analysis_description = \
        analysis_description[0]
    sample_igf_id = \
      analysis_description.get('sample_igf_id')
    if analysis_description.get('cell_marker_list') is not None:
      cell_marker_list = \
        analysis_description.get('cell_marker_list')                            # reset cell marker list
      check_file_path(cell_marker_list)                                         # check filepath
    if analysis_description.get('s_genes') is not None:
      s_genes = \
        analysis_description.get('s_genes')
      if not isinstance(s_genes, list):
        raise TypeError(
          'Expecting a list for s_genes and got {0}'.\
            format(type(s_genes)))
    if analysis_description.get('g2m_genes') is not None:
      g2m_genes = \
        analysis_description.get('g2m_genes')
      if not isinstance(g2m_genes, list):
        raise TypeError(
          'Expecting a list for g2m_genes and got {0}'.\
            format(type(g2m_genes)))
    #genome_build = \
    #  analysis_description[0].get('genome_build')
    sa = SampleAdaptor(**dbparams)
    sa.start_session()
    genome_build = \
      sa.fetch_sample_species_name(
        sample_igf_id=sample_igf_id)
    sa.close_session()
    tmp_dir = get_temp_dir(use_ephemeral_space=True)
    input_params = {
      'DATE_TAG': get_date_stamp(),
      'PROJECT_IGF_ID': project_igf_id,
      'SAMPLE_IGF_ID': sample_igf_id,
      'CELLRANGER_COUNT_DIR': cellranger_count_dir,
      'CELLRANGER_VDJ_DIR': cellranger_vdj_dir,
      'CELL_MARKER_LIST': cell_marker_list,
      'CUSTOM_S_GENES_LIST': s_genes,
      'CUSTOM_G2M_GENES_LIST': g2m_genes,
      'GENOME_BUILD': genome_build}
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
      cellbrowser_html_dir = \
        os.path.join(
          tmp_dir,
          'cellbrowser_html_{0}'.format(get_datestamp_label()))                 # adding datestamp label to cellbrowser html dir path
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
    output_notebook_path, notebook_cmd = \
      nb.execute_notebook_in_singularity()
    ti.xcom_push(
      key=output_notebook_key,
      value=output_notebook_path)
    if analysis_name == 'SCANPY':
      ti.xcom_push(
        key=output_cellbrowser_key,
        value=cellbrowser_html_dir)
    message = \
      'Generated notebook for analysis: {0}, command: {1}'.\
        format(analysis_name,notebook_cmd)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      asana_conf=ASANA_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      asana_project_id=ASANA_PROJECT,
      project_id=project_igf_id,
      comment=message,
      reaction='pass')
  except Exception as e:
    logging.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise ValueError(e)


def irods_files_upload_for_analysis(**context):
  try:
    ti = context.get('ti')
    xcom_pull_task = \
      context['params'].get('xcom_pull_task')
    xcom_pull_files_key = \
      context['params'].get('xcom_pull_files_key')
    collection_name_key = \
      context['params'].get('collection_name_key')
    collection_name_task = \
      context['params'].get('collection_name_task')
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
    analysis_db_records = \
      aa.fetch_analysis_records_analysis_id(
        analysis_id=int(analysis_id),
        output_mode='one_or_none')
    aa.close_session()
    analysis_db_name = None
    if analysis_db_records is not None:
      analysis_db_name = \
        analysis_db_records.analysis_name
    file_list_for_copy = \
      ti.xcom_pull(
        task_ids=xcom_pull_task,
        key=xcom_pull_files_key)
    collection_name = \
      ti.xcom_pull(
        task_ids=collection_name_task,
        key=collection_name_key)
    pa = ProjectAdaptor(**dbparams)
    pa.start_session()
    user = \
      pa.fetch_data_authority_for_project(
        project_igf_id=project_igf_id)                                        # fetch user info from db
    if user is None:
        raise ValueError(
                'No user found for project {0}'.\
                  format(project_igf_id))
    username = user.username                                                  # get username for irods
    pa.close_session()
    irods_upload = IGF_irods_uploader(IRDOS_EXE_DIR)
    for file in file_list_for_copy:
      check_file_path(file)
    dir_path_list = \
      ['analysis',context['task'].dag_id]
    if analysis_db_name is not None:
      dir_path_list.\
        append(analysis_db_name)
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
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
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
      aa.fetch_project_igf_id_for_analysis_id(
        analysis_id=int(analysis_id))
    analysis_db_records = \
      aa.fetch_analysis_records_analysis_id(
        analysis_id=int(analysis_id),
        output_mode='one_or_none')
    aa.close_session()
    analysis_db_name = None
    if analysis_db_records is not None:
      analysis_db_name = \
        analysis_db_records.analysis_name
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
    dest_dir_list = [
      ftp_project_path,
      project_igf_id,
      'analysis',
      context['task'].dag_id]
    if analysis_db_name is not None:
      dest_dir_list.\
        append(analysis_db_name)
    dest_dir_list.\
        append(collection_name)
    destination_output_path = \
      os.path.join(*dest_dir_list)
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
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
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
    html_collection_type = \
      context['params'].get('html_collection_type')
    #genome_column = \
    #  context['params'].get('genome_column')
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
    #genome_build = \
    #  analysis_description[0].get(genome_column)
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
    sa = SampleAdaptor(**dbparams)
    sa.start_session()
    genome_build = \
      sa.fetch_sample_species_name(
        sample_igf_id=sample_igf_id)
    sa.close_session()
    au = \
      Analysis_collection_utils(
        dbsession_class=sa.get_session_class(),
        analysis_name=analysis_name,
        tag_name=genome_build,
        collection_name=sample_igf_id,
        collection_type=collection_type,
        collection_table=collection_table,
        base_path=base_result_dir)
    output_file_list = \
      au.load_file_to_disk_and_db(
        input_file_list=[temp_archive_name],
        withdraw_exisitng_collection=True)                                      # loading cellranger output files without BAM
    au = \
      Analysis_collection_utils(
        dbsession_class=sa.get_session_class(),
        analysis_name=analysis_name,
        tag_name=genome_build,
        collection_name=sample_igf_id,
        collection_type=html_collection_type,
        collection_table=collection_table,
        base_path=base_result_dir)
    output_html_file = \
      au.load_file_to_disk_and_db(
        input_file_list=[html_report_filepath],
        withdraw_exisitng_collection=True)                                      # loading cellranger html file
    if len(output_html_file)==0:
      raise ValueError(
              'No html file found after file load, sample: {0}'.\
                format(sample_igf_id))
    ti.xcom_push(
      key=output_xcom_key,
      value=output_file_list)
    ti.xcom_push(
      key=xcom_collection_name_key,
      value=sample_igf_id)
    ti.xcom_push(
      key=html_xcom_key,
      value=output_html_file[0])
  except Exception as e:
    logging.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
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
    convert_cellranger_bam_to_cram = \
      context['params'].get('convert_cellranger_bam_to_cram_task')
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
        append(convert_cellranger_bam_to_cram)
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
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
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


def load_cellranger_metrices_to_collection(**context):
  try:
    ti = context.get('ti')
    cellranger_xcom_key = \
      context['params'].get('cellranger_xcom_key')
    cellranger_xcom_pull_task = \
      context['params'].get('cellranger_xcom_pull_task')
    collection_type = \
      context['params'].get('collection_type')
    collection_name_task = \
      context['params'].get('collection_name_task')
    collection_name_key = \
      context['params'].get('collection_name_key')
    metrics_summary_file = \
      context['params'].get('metrics_summary_file')
    attribute_prefix = \
      context['params'].get('attribute_prefix')
    attribute_name_key = \
      context['params'].get('attribute_name_key','attribute_name')
    attribute_value_key = \
      context['params'].get('attribute_value_key','attribute_value')
    cellranger_output_path = \
      ti.xcom_pull(
        task_ids=cellranger_xcom_pull_task,
        key=cellranger_xcom_key)
    collection_name = \
      ti.xcom_pull(
        task_ids=collection_name_task,
        key=collection_name_key)
    metrics_file_path = \
      os.path.join(
        cellranger_output_path,
        metrics_summary_file)
    attribute_data = \
      _build_collection_attribute_data_for_cellranger(
        metrics_file=metrics_file_path,
        collection_name=collection_name,
        collection_type=collection_type,
        attribute_name=attribute_name_key,
        attribute_value=attribute_value_key,
        attribute_prefix=attribute_prefix)
    dbparams = \
      read_dbconf_json(DATABASE_CONFIG_FILE)
    ca = CollectionAdaptor(**dbparams)
    ca.start_session()
    try:
      ca.create_or_update_collection_attributes(
        data=attribute_data,
        autosave=False)                                                         # load cellranger metrics to collection attribute table
      ca.commit_session()
      ca.close_session()
    except:
      ca.rollback_session()
      ca.close_session()
      raise
  except Exception as e:
    logging.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise ValueError(e)


def _build_collection_attribute_data_for_cellranger(
      metrics_file,collection_name,collection_type,attribute_name='attribute_name',
      attribute_value='attribute_value',attribute_prefix=None):
  """
  An internal function for building collection attribute data for cellranger
  metrics summary output

  :param metrics_file: Metrics summary filepath
  :param collection_name: Collection name
  :param collection_type: Collection_type
  :param attribute_name: Attribute name, default attribute_name
  :param attribute_value: Attribute value, default attribute_value
  :param attribute_prefix: Optional string for attribute prefix, default None
  :returns: A list of dictionary wiuth thecollection attribute data
  """
  try:
    check_file_path(metrics_file)
    attribute_data = \
      pd.read_csv(metrics_file).T.\
        reset_index()
    attribute_data.columns = [
      attribute_name,
      attribute_value]
    if attribute_prefix is None:
      attribute_data[attribute_name] = \
        attribute_data[attribute_name].\
          map(lambda x: x.replace(' ','_'))
    else:
      attribute_data[attribute_name] = \
        attribute_data[attribute_name].\
          map(lambda x: \
              '{0}_{1}'.format(\
                attribute_prefix,
                x.replace(' ','_')))
    attribute_data['name'] = collection_name
    attribute_data['type'] = collection_type
    attribute_data[attribute_value] = \
      attribute_data[attribute_value].astype(str)
    attribute_data[attribute_value] = \
      attribute_data[attribute_value].\
        map(lambda x: \
              x.replace(',',''))
    attribute_data = \
      attribute_data.\
        to_dict(orient='records')
    return attribute_data
  except Exception as e:
    raise ValueError(
            'Failed to build collection attribute data for collection {0}:{1}, error: {2}'.\
              format(collection_name,collection_type,e))


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
    dbparams = \
      read_dbconf_json(DATABASE_CONFIG_FILE)
    dag_run = context.get('dag_run')
    if dag_run is None or \
       dag_run.conf is None or \
       dag_run.conf.get('analysis_id') is None:
      raise ValueError('No analysis id found for collection')
    analysis_id = \
      dag_run.conf.get('analysis_id')
    aa = \
      AnalysisAdaptor(**dbparams)
    aa.start_session()
    project_igf_id = \
      aa.fetch_project_igf_id_for_analysis_id(
        analysis_id=int(analysis_id))                                           # fetch project id for analysis
    aa.close_session()
    analysis_description = \
      ti.xcom_pull(
        task_ids=analysis_description_xcom_pull_task,
        key=analysis_description_xcom_key)
    library_csv = \
      ti.xcom_pull(
        task_ids=library_csv_xcom_pull_task,
        key=library_csv_xcom_key)
    cellranger_exe = CELLRANGER_EXE
    job_timeout = CELLRANGER_JOB_TIMEOUT
    output_dir = get_temp_dir(use_ephemeral_space=True)
    sample_id = None
    for entry in analysis_description:
      sample_igf_id = entry.get('sample_igf_id')
      if sample_id is None:
        sample_id = sample_igf_id
      else:
        sample_id = '{0}_{1}'.format(sample_id, sample_igf_id)
    message = 'Started cellranger run, output path: {0}'.format(output_dir)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='pass')
    cellranger_multi_cmd, output_dir = \
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
    message = \
      'Finished cellranger multi {0}: {1}'.\
        format(sample_id, cellranger_multi_cmd)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      asana_conf=ASANA_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      asana_project_id=ASANA_PROJECT,
      project_id=project_igf_id,
      comment=message,
      reaction='pass')
  except Exception as e:
    logging.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
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
      context['params'].get('r1-length')
    r2_length = \
      context['params'].get('r2-length')
    fastq_input_dir_tag = \
      context['params'].get('fastq_input_dir_tag')
    fastq_output_dir_tag = \
      context['params'].get('fastq_output_dir_tag')
    use_ephemeral_space = \
      context['params'].get('use_ephemeral_space')
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
      use_ephemeral_space=use_ephemeral_space,
      singularity_image=singularity_image)
  except Exception as e:
    logging.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise ValueError(e)


def _get_fastq_and_run_cutadapt_trim(
      analysis_info,analysis_description,analysis_name,run_id,
      fastq_input_dir_tag,fastq_output_dir_tag,singularity_image,
      use_ephemeral_space=False,r1_length=0,r2_length=0,
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
  :param use_ephemeral_space: Toggle for using ephimeral space for temp files
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
    if 'r1-length' in analysis_entry.columns:
      r1_length = analysis_entry['r1-length'].values[0]                         # reset r1-length
    if 'r2-length' in analysis_entry.columns:
      r2_length = analysis_entry['r2-length'].values[0]                         # reset r2-length
    run = sample_info.get('runs').get(str(run_id))
    if run is None:
      raise ValueError(
              'No run {0} found for feature {1} in analysis_info'.\
                format(run,analysis_name))
    if isinstance(cutadapt_options,tuple):
      cutadapt_options = list(cutadapt_options)
    input_fastq_dir = run.get(fastq_input_dir_tag)
    output_fastq_dir = run.get(fastq_output_dir_tag)
    temp_output_dir = \
      get_temp_dir(use_ephemeral_space=use_ephemeral_space)
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
        temp_fastq_file = \
          os.path.join(temp_output_dir,fastq)
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
                read1_fastq_out=temp_fastq_file,
                cutadapt_options=cutadapt_options_r1,
                cutadapt_exe=cutadapt_exe,
                dry_run=dry_run,
                singularity_image_path=singularity_image)
            if not dry_run:
              copy_local_file(
                temp_fastq_file,
                output_fastq_file,
                force=True)
          else:
            copy_local_file(
              input_fastq_file,
              output_fastq_file,
              force=True)
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
                read1_fastq_out=temp_fastq_file,
                cutadapt_options=cutadapt_options_r2,
                cutadapt_exe=cutadapt_exe,
                dry_run=dry_run,
                singularity_image_path=singularity_image)
            if not dry_run:
              copy_local_file(
                temp_fastq_file,
                output_fastq_file,
                force=True)
          else:
            copy_local_file(
              input_fastq_file,
              output_fastq_file,
              force=True)
        if re.match(index_file_name_pattern,fastq):
          # copy I1 or I2
          copy_local_file(
            input_fastq_file,
            output_fastq_file,
            force=True)
  except Exception as e:
    raise ValueError(
            'Failed to trim or copy reads, error: {0}'.\
              format(e))


def configure_cellranger_run_func(**context):
  try:
    ti = context.get('ti')
    #
    # xcop_pull analysis_description
    # xcom_pull analysis_info
    #
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
    #
    # generate library.csv file for cellranger run
    #
    csv_path = \
      _create_library_csv_for_cellranger_multi(
        analysis_description=analysis_description,
        analysis_info=analysis_info,
        work_dir=work_dir)
    #
    # push the csv path to xcom
    #
    ti.xcom_push(
      key=library_csv_xcom_key,
      value=csv_path)
    #
    # log
    #
    message = \
      'Generated temp lib file {0} for cellranger multi'.\
        format(csv_path)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='pass')
  except Exception as e:
    logging.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
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
        'include-introns',
        'no-secondary',
        'no-bam',
        'min-assignment-confidence'],
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
      feature_types = list(FEATURE_TYPE.keys())
      analysis_description_df = pd.DataFrame(analysis_description)
      if 'sample_igf_id' not in analysis_description_df.columns:
        raise KeyError('Key sample_igf_id not in analysis_description')
      sample_igf_id_list = \
        analysis_description_df['sample_igf_id'].values.tolist()
      #
      # check if sample and analysis are from the same project
      #
      _check_sample_id_and_analysis_id_for_project(
        analysis_id=analysis_id,
        sample_igf_id_list=sample_igf_id_list,
        dbconfig_file=DATABASE_CONFIG_FILE)
      #
      # add reference genome paths if reference type and genome build is present
      # check for genome build info
      # INPUT:
      # analysis_description = [{
      #   'sample_igf_id':'IGF001',
      #   'feature_type':'gene expression',
      #   'reference_type':'TRANSCRIPTOME_TENX',
      #   'r1-length':26,
      #   'r2-length':0,                          # optional, 0 for no trimming
      #   'cell_annotation_csv':'/path/csv',      # optional, cell annotation file
      #   'genome_build':'HG38' }]
      #  OUTPUT:
      # analysis_description = [{
      #   'sample_igf_id':'IGF001',
      #   'feature_type':'gene expression',
      #   'reference_type':'TRANSCRIPTOME_TENX',
      #   'r1-length':26,
      #   'r2-length':0,                          # optional, 0 for no trimming
      #   'cell_annotation_csv':'/path/csv',      # optional, cell annotation file
      #   'reference':'/path/ref',
      #   'genome_build':'HG38' }]
      #
      analysis_description = \
        _add_reference_genome_path_for_analysis(
          database_config_file=database_config_file,
          analysis_description=analysis_description,
          genome_required=True)
      #
      # check the analysis description and sample validity
      # warn if multiple samples are allocated to same sub category
      # filter analysis branch list
      # INPUT: formatted analysis description with reference column
      # OUTPUT: a list of samples, a list of analysis and a list of errors
      #
      sample_id_list, analysis_list, messages = \
        _validate_analysis_description(
          analysis_description=analysis_description,
          feature_types=feature_types)
      if len(messages) > 0:
        raise ValueError('Analysis validation failed: {0}'.\
                format(messages))
      #
      # get the fastq paths for sample ids and set the trim output dirs per run
      #
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
      #   'r1-length':26,
      #   'r2-length':0,                          # optional, 0 for no trimming
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
      #
      # mark analysis_id as running,if its not already running
      #
      status = \
        _check_and_mark_analysis_seed(
          analysis_id=analysis_id,
          anslysis_type=analysis_type,
          new_status='RUNNING',
          no_change_status='RUNNING',
          database_config_file=database_config_file)
      #
      # xcom push analysis_info and analysis_description
      #
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
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise ValueError(e)


def _check_sample_id_and_analysis_id_for_project(
      analysis_id,sample_igf_id_list,dbconfig_file):
  '''
  An internal method for checking the consistency of sample ids and analysis records

  :param analysis_id: Analysis id
  :param sample_igf_id_list: A list of sample_igf_id
  :returns: None
  '''
  try:
    dbparams = read_dbconf_json(dbconfig_file)
    sa = SampleAdaptor(**dbparams)
    sa.start_session()
    project_igf_id_list = \
      sa.get_project_ids_for_list_of_samples(
        sample_igf_id_list=sample_igf_id_list)
    aa = AnalysisAdaptor(**{'session':sa.session})
    project_igf_id = \
      aa.fetch_project_igf_id_for_analysis_id(
        analysis_id=int(analysis_id))
    sa.close_session()
    if len(project_igf_id_list)>1:
      raise ValueError(
              'More than one project found for sample list, projects: {0}'.\
                format(project_igf_id_list))
    if len(project_igf_id_list)==1 and \
       project_igf_id_list[0]!=project_igf_id:
      raise ValueError(
              'Analysis is linked to project {0} and samples are linked to poject {1}'.\
                format(project_igf_id,project_igf_id_list))
  except Exception as e:
    raise ValueError(
            'Failed sample and project consistancy check, error: {0}'.\
              format(e))


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