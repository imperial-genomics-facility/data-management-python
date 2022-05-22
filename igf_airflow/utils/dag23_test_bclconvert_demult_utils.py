import os
import json
import logging
import pandas as pd
from typing import Tuple
from airflow.models import Variable
from igf_data.utils.fileutils import get_temp_dir
from igf_data.utils.dbutils import read_dbconf_json
from igf_airflow.logging.upload_log_msg import send_log_to_channels
from igf_portal.api_utils import upload_files_to_portal
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.illumina.samplesheet import SampleSheet
from igf_airflow.utils.dag22_bclconvert_demult_utils import _check_and_seed_seqrun_pipeline
from igf_data.process.singlecell_seqrun.processsinglecellsamplesheet import ProcessSingleCellSamplesheet
from igf_data.process.singlecell_seqrun.processsinglecellsamplesheet import ProcessSingleCellDualIndexSamplesheet

SLACK_CONF = Variable.get('slack_conf',default_var=None)
MS_TEAMS_CONF = Variable.get('ms_teams_conf',default_var=None)
HPC_SEQRUN_BASE_PATH = Variable.get('hpc_seqrun_path', default_var=None)
DATABASE_CONFIG_FILE = Variable.get('database_config_file', default_var=None)
SINGLECELL_BARCODE_JSON = Variable.get('singlecell_barcode_json', default_var=None)
SINGLECELL_DUAL_BARCODE_JSON = Variable.get('singlecell_dual_barcode_json', default_var=None)
BCLCONVERT_IMAGE = Variable.get('bclconvert_image_path', default_var=None)
INTEROP_NOTEBOOK_IMAGE = Variable.get('interop_notebook_image_path', default_var=None)
BCLCONVERT_REPORT_TEMPLATE = Variable.get('bclconvert_report_template', default_var=None)
BCLCONVERT_REPORT_LIBRARY = Variable.get("bclconvert_report_library", default_var=None)
BOX_DIR_PREFIX = Variable.get('box_dir_prefix_for_seqrun_report', default_var=None)
BOX_CONFIG_FILE  = Variable.get('box_config_file', default_var=None)
IGF_PORTAL_CONF = Variable.get('igf_portal_conf', default_var=None)

log = logging.getLogger(__name__)

def upload_report_to_box_func(**context):
  try:
    pass
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

def generate_report_func(**context):
  try:
    pass
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

def bcl_convert_run_func(**context):
  try:
    pass
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

def _format_samplesheet_per_index_group(
  samplesheet_file: str,
  singlecell_barcode_json: str,
  singlecell_dual_barcode_json: str,
  platform: str,
  single_cell_tag: str = '10X',
  index2_rule: str = 'NO_CHANGE',
  override_cycles: str = '') -> dict:
  try:
    tmp_dir = get_temp_dir()
    tmp_samplesheet1 = \
      os.path.join(
        tmp_dir,
        os.path.basename(f'temp1_{samplesheet_file}'))
    sc_dual_process = \
      ProcessSingleCellDualIndexSamplesheet(
        samplesheet_file=samplesheet_file,
        singlecell_dual_index_barcode_json=singlecell_dual_barcode_json,
        platform=platform,
        index2_rule=index2_rule)
    sc_dual_process.\
      modify_samplesheet_for_sc_dual_barcode(
        output_samplesheet=tmp_samplesheet1)
    tmp_samplesheet2 = \
      os.path.join(
        tmp_dir,
        os.path.basename(f'temp2_{samplesheet_file}'))
    sc_data = \
      ProcessSingleCellSamplesheet(
        tmp_samplesheet1,
        singlecell_barcode_json,
        single_cell_tag)
    sc_data.\
      change_singlecell_barcodes(
        tmp_samplesheet2)
    # group by index
    sa = SampleSheet(tmp_samplesheet2)
    df = pd.DataFrame(sa._data)
    if 'Lane' in df.columns:
      pass
    else:
      pass
  except Exception as e:
    raise ValueError(f"Failed to format samplesheet per index group, error: {e}")


def get_formatted_samplesheets_func(**context):
  try:
    ti = context.get('ti')
    samplesheet_xcom_key = \
      context['params'].get('samplesheet_xcom_key', 'samplesheet_data')
    samplesheet_xcom_task = \
      context['params'].get('samplesheet_xcom_task', 'get_samplesheet_from_portal')
    samplesheet_tag = \
      context['params'].get('samplesheet_tag', 'samplesheet_tag')
    samplesheet_file = \
      context['params'].get('samplesheet_file', 'samplesheet_file')
    samplesheet_data = \
      ti.xcom_pull(
        task_ids=samplesheet_xcom_task,
        key=samplesheet_xcom_key)
    if not isinstance(samplesheet_data, dict) or \
       samplesheet_tag not in samplesheet_data or \
       samplesheet_file not in samplesheet_data:
      raise ValueError(
        'samplesheet_data is not in the correct format')
    samplesheet_tag_name = samplesheet_data.get(samplesheet_tag)
    samplesheet_file_path = samplesheet_data.get(samplesheet_file)
    # TO Do following
    # * get index 2 rule from db
    # * split samplesheet per index group
    # * create a merged version of samplesheet
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
    dag_run = context.get('dag_run')
    next_task = context['params'].get('next_task')
    last_task = context['params'].get('last_task')
    seed_status = context['params'].get('seed_status')
    no_change_status = context['params'].get('no_change_status')
    seed_table = context['params'].get('seed_table')
    seqrun_id = None
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('seqrun_id') is not None:
      seqrun_id = \
        dag_run.conf.get('seqrun_id')
    if seqrun_id is None:
      raise ValueError('seqrun_id is not found in dag_run.conf')
    status = \
      _check_and_seed_seqrun_pipeline(
        seqrun_id=seqrun_id,
        pipeline_name=context['task'].dag_id,
        dbconf_json_path=DATABASE_CONFIG_FILE,
        seed_status=seed_status,
        seed_table=seed_table,
        no_change_status=no_change_status)
    if status:
      return [next_task]
    else:
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


def get_samplesheet_from_portal_func(**context):
  try:
    ti = context.get('ti')
    samplesheet_xcom_key = \
      context['params'].get('samplesheet_xcom_key', 'samplesheet_data')
    samplesheet_tag = \
      context['params'].get('samplesheet_tag', 'samplesheet_tag')
    samplesheet_file = \
      context['params'].get('samplesheet_file', 'samplesheet_file')
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
      value={samplesheet_tag: samplesheet_tag, samplesheet_file: samplesheet_file})
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