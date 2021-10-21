import logging, os, requests, subprocess
from tempfile import tempdir
import pandas as pd
from airflow.models import Variable
from igf_airflow.logging.upload_log_msg import send_log_to_channels
from igf_data.utils.fileutils import check_file_path, get_temp_dir, copy_local_file
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.utils.tools.reference_genome_utils import Reference_genome_utils
from igf_data.igfdb.baseadaptor import BaseAdaptor


GENOME_FASTA_TYPE = 'GENOME_FASTA'
GENE_REFFLAT_TYPE = 'GENE_REFFLAT'
RIBOSOMAL_INTERVAL_TYPE = 'RIBOSOMAL_INTERVAL'
DATABASE_CONFIG_FILE = Variable.get('database_config_file', default_var=None)
SLACK_CONF = Variable.get('slack_conf', default_var=None)
MS_TEAMS_CONF = Variable.get('ms_teams_conf', default_var=None)
STAR_EXE = Variable.get('star_exe', default_var=None)
STAR_REF_PATH = Variable.get('star_exe', default_var=None)
RSEM_EXE = Variable.get('rsem_exe', default_var=None)
RSEM_REF_PATH = Variable.get('rsem_ref_path', default_var=None)
BIGBED_REF_PATH = Variable.get('bigbed_ref_path', default_var=None)
UCSC_EXE_DIR = Variable.get('ucsc_exe_dir', default_var=None)
REFFLAT_REF_PATH = Variable.get('refflat_ref_path', default_var=None)
CELLRANGER_EXE = Variable.get('cellranger_exe', default_var=None)
CELLRANGER_REF_PATH = Variable.get('cellranger_ref_path', default_var=None)

def download_file(url, output_file):
  try:
    with requests.get(url, stream=True) as r:
      r.raise_for_status()
      with open(output_file, 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192):
          f.write(chunk)
    if not os.path.exists(output_file):
      raise ValueError('Output file {0} is missing'.format(output_file))
  except Exception as e:
    raise ValueError('Failed to download file, error{0}'.format(e))


def download_gtf_file_func(**context):
  try:
    dag_run = context.get('dag_run')
    ti = context.get('ti')
    gtf_xcom_key = \
      context['params'].get('gtf_xcom_key')
    if dag_run is not None and \
       dag_run.conf is not None:
      gtf_url = dag_run.conf.get('gtf_url')
      if gtf_url is None:
        raise ValueError('No url found for GTF')
      temp_dir = \
        get_temp_dir(use_ephemeral_space=True)
      filename = gtf_url.split('/')[-1]
      temp_file_path = \
        os.path.join(temp_dir, filename)
      download_file(
        url=gtf_url,
        output_file=temp_file_path)
      ti.xcom_push(
        key=gtf_xcom_key,
        value=temp_file_path)
    else:
      raise ValueError('No dag_run.conf entry found')
  except Exception as e:
    logging.error(e)
    message = \
      'SampleSheet check error: {0}'.\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise


def fetch_ref_genome_from_db(dbconfig_file, genome_build, reference_type):
  try:
    check_file_path(dbconfig_file)
    dbparams = \
      read_dbconf_json(dbconfig_file)
    base = BaseAdaptor(**dbparams)
    ref_tool = \
      Reference_genome_utils(
        dbsession_class=base.get_session_class(),
        genome_tag=genome_build)
    reference_file = \
      ref_tool.\
        _fetch_collection_files(
          collection_type=reference_type,
          check_missing=False)
    return reference_file
  except Exception as e:
    raise ValueError(
            'Failed to fetch file for {0} {1}, error: {2}'.\
              format(genome_build, reference_type, e))


def create_star_index_func(**context):
  try:
    gtf_xcom_key = \
      context['params'].get('gtf_xcom_key')
    gtf_xcom_task = \
      context['params'].get('gtf_xcom_task')
    threads = \
      context['params'].get('threads')
    star_options = \
      context['params'].get('star_options')
    star_ref_xcom_key = \
      context['params'].get('star_ref_xcom_key')
    dag_run = context.get('dag_run')
    ti = context.get('ti')
    gtf_file = \
      ti.xcom_pull(
        task_ids=gtf_xcom_task,
        key=gtf_xcom_key)
    if dag_run is not None and \
       dag_run.conf is not None:
      tag = dag_run.conf.get('tag')
      species_name = dag_run.conf.get('species_name')
      if species_name is None or \
         tag is None:
        raise ValueError('Missing species name or tag')
      fasta_file = \
        fetch_ref_genome_from_db(
          dbconfig_file=DATABASE_CONFIG_FILE,
          genome_build=species_name,
          reference_type=GENOME_FASTA_TYPE)
      temp_dir = \
        get_temp_dir(use_ephemeral_space=True)
      genome_dir = \
        os.path.join(
          get_temp_dir,
          '{0}_{1}_star'.\
            format(species_name, tag))
      star_cmd = [
        STAR_EXE,
        '--runThreadN', int(threads),
        '--runMode', 'genomeGenerate',
        '-genomeDir', genome_dir,
        '--genomeFastaFiles', fasta_file,
        '--sjdbGTFfile', gtf_file]
      if star_options is not None and \
         isinstance(star_options, list) and \
         len(star_options) > 0:
        star_cmd.\
          extend(star_options)
      subprocess.\
        check_call(
          ' '.join(star_cmd),
          shell=True)
      ti.xcom_push(
        key=star_ref_xcom_key,
        value=genome_dir)
    else:
      raise ValueError('No dag_run.conf entry found')
  except Exception as e:
    logging.error(e)
    message = \
      'SampleSheet check error: {0}'.\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise