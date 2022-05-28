import os
import json
import logging
import pandas as pd
from typing import Tuple
from airflow.models import Variable
from igf_data.utils.fileutils import copy_local_file, get_temp_dir
from igf_data.utils.fileutils import check_file_path
from igf_data.utils.dbutils import read_dbconf_json
from igf_airflow.logging.upload_log_msg import send_log_to_channels
from igf_portal.api_utils import upload_files_to_portal
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.utils.sequtils import rev_comp
from igf_airflow.utils.dag22_bclconvert_demult_utils import _check_and_seed_seqrun_pipeline
from igf_data.process.singlecell_seqrun.processsinglecellsamplesheet import ProcessSingleCellSamplesheet
from igf_data.process.singlecell_seqrun.processsinglecellsamplesheet import ProcessSingleCellDualIndexSamplesheet
from igf_airflow.utils.dag22_bclconvert_demult_utils import bclconvert_singularity_wrapper
from igf_airflow.utils.dag22_bclconvert_demult_utils import _calculate_bases_mask
from igf_airflow.utils.dag22_bclconvert_demult_utils import generate_bclconvert_report
from igf_data.utils.box_upload import upload_file_or_dir_to_box

log = logging.getLogger(__name__)

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
BOX_CONFIG_FILE  = Variable.get('box_config_file', default_var=None)

def create_demult_report_for_portal(
      samplesheet_data: str,
      demult_data: str,
      index_column: str = 'index',
      demult_dir_column: str = 'demult_dir',
      lane_column: str = 'lane',
      tag_column: str = 'tag') -> str:
  try:
    samplesheet_df = pd.DataFrame(samplesheet_data)
    if not index_column in samplesheet_df.columns or \
       not lane_column in samplesheet_df.columns or \
       not tag_column in samplesheet_df.columns:
      raise ValueError(
        'index_column, lane_column and tag_column must be in samplesheet_df.columns')
    demult_df = pd.DataFrame(demult_data)
    if not demult_dir_column in demult_df.columns or \
       not index_column in demult_df.columns:
      raise ValueError(
        'demult_dir_column and index_column must be in demult_df.columns')
    merged_samplesheet_df = \
      samplesheet_df[samplesheet_df[tag_column]=='merged']
    if not len(merged_samplesheet_df):
      raise ValueError('No merged samplesheet found')
    for index in merged_samplesheet_df[index_column].values.tolist():
      filtered_demult_df = demult_df[demult_df[index_column]==index]
      if len(filtered_demult_df)==0:
        log.warning(f'No demult data found for index {index}')
      else:
        demult_dir = \
          filtered_demult_df[demult_dir_column].values[0]
        demult_reports_dir = \
          os.path.join(demult_dir, 'Reports')
        check_file_path(demult_reports_dir)
      ## TO DO # update
  except Exception as e:
    raise ValueError(
      f'Failed to create combined demult report for portal, error: {e}')


def generate_merged_report_func(**context):
  try:
    ti = context.get('ti')
    dag_run = context.get('dag_run')
    demult_info_key = \
      context['params'].\
      get(
        'demult_info_key',
        'demult_info')
    all_upstream_tasks = \
      context['task'].\
      get_direct_relative_ids(upstream=True)
    all_demult_info = list()
    for task_id in all_upstream_tasks:
      xcom_data = \
        ti.xcom_pull(
          task_ids=task_id,
          key=demult_info_key)
      if xcom_data is not None:
        all_demult_info.\
          append(xcom_data)
    # get seqrun id
    seqrun_id = None
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('seqrun_id') is not None:
      seqrun_id = dag_run.conf.get('seqrun_id')
    if seqrun_id is None:
      raise ValueError('seqrun_id is not provided')
    # get formatted samplesheets
    formatted_samplesheet_xcom_key = \
      context['params'].\
      get(
        'formatted_samplesheet_xcom_key',
        'formatted_samplesheet_data')
    formatted_samplesheet_xcom_task = \
      context['params'].\
      get(
        'formatted_samplesheet_xcom_task',
        'get_formatted_samplesheets')
    formatted_samplesheet_data = \
      ti.xcom_pull(
        task_ids=formatted_samplesheet_xcom_task,
        key=formatted_samplesheet_xcom_key)
    if len(all_demult_info) != len(formatted_samplesheet_data):
      log.warn(
        'Number of samplesheets: {0}, number of demultiplexing dir: {1}'.\
          format(
            len(formatted_samplesheet_data),
            len(all_demult_info)))
    # merge demultiplexing reports
    merged_report = \
      create_demult_report_for_portal(
        samplesheet_data = formatted_samplesheet_data,
        demult_data = all_demult_info)
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


def upload_report_to_box_func(**context):
  try:
    ti = context.get('ti')
    dag_run = context.get('dag_run')
    # get demult report
    demult_report_key = \
      context['params'].\
      get(
        'demult_report_key',
        'demult_report')
    demult_report_task = \
      context['params'].\
      get('demult_report_task')
    demult_report = \
      ti.xcom_pull(
        task_ids=demult_report_task,
        key=demult_report_key)
    check_file_path(demult_report)
    # get seqrun id
    seqrun_id = None
    samplesheet_tag = None
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('seqrun_id') is not None:
      seqrun_id = dag_run.conf.get('seqrun_id')
      samplesheet_tag = dag_run.conf.get('samplesheet_tag')
    if seqrun_id is None:
      raise ValueError('seqrun_id is not provided')
    if samplesheet_tag is None:
      raise ValueError('samplesheet_tag is not provided')
    # get formatted samplesheets
    formatted_samplesheet_xcom_key = \
      context['params'].\
      get(
        'formatted_samplesheet_xcom_key',
        'formatted_samplesheet_data')
    formatted_samplesheet_xcom_task = \
      context['params'].\
      get(
        'formatted_samplesheet_xcom_task',
        'get_formatted_samplesheets')
    samplesheet_index = \
      context['params'].\
      get('samplesheet_index')
    index_column = \
      context['params'].\
      get('index_column', 'index')
    lane_column = \
      context['params'].\
      get('lane_column', 'lane')
    tag_column = \
      context['params'].\
      get('tag_column', 'tag')
    formatted_samplesheet_data = \
      ti.xcom_pull(
        task_ids=formatted_samplesheet_xcom_task,
        key=formatted_samplesheet_xcom_key)
    if not isinstance(formatted_samplesheet_data) or \
       len(formatted_samplesheet_data) == 0:
      raise ValueError('formatted_samplesheet_data is empty')
    df = pd.DataFrame(formatted_samplesheet_data)
    df[index_column] = \
      df[index_column].\
        astype(str)
    filtered_df = \
      df[df[index_column]==samplesheet_index]
    if len(filtered_df)==0:
      raise ValueError('No samplesheet index found in the samplesheet')
    lane_id = filtered_df[lane_column].values[0]
    tag = filtered_df[tag_column].values[0]
    # upload file to box
    box_dir = \
      os.path.join(
        BOX_DIR_PREFIX,
        seqrun_id,
        samplesheet_tag,
        lane_id,
        tag)
    upload_file_or_dir_to_box(
      box_config_file=BOX_CONFIG_FILE,
      file_path=demult_report,
      upload_dir=box_dir)
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
    ti = context.get('ti')
    dag_run = context.get('dag_run')
    demult_report_key = \
      context['params'].\
      get(
        'demult_report_key',
        'demult_report')
    demult_dir_key = \
      context['params'].\
      get(
        'demult_dir_key',
        'demult_dir')
    demult_dir_task = \
      context['params'].\
      get('demult_dir_task')
    demult_dir = \
      ti.xcom_pull(
        task_ids=demult_dir_task,
        key=demult_dir_key)
    check_file_path(demult_dir)
    bclconvert_reports_path = \
      os.path.join(
        demult_dir,
        'Reports')
    check_file_path(bclconvert_reports_path)
    # get seqrun path and samplesheet tag
    seqrun_id = None
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('seqrun_id') is not None:
      seqrun_id = \
        dag_run.conf.get('seqrun_id')
    if seqrun_id is None:
      raise ValueError('seqrun_id is not found in dag_run.conf')
    seqrun_path = \
      os.path.join(HPC_SEQRUN_BASE_PATH, seqrun_id)
    # generate report
    temp_dir = \
      get_temp_dir(use_ephemeral_space=True)
    report_file = \
      generate_bclconvert_report(
        seqrun_path=seqrun_path,
        image_path=INTEROP_NOTEBOOK_IMAGE,
        report_template=BCLCONVERT_REPORT_TEMPLATE,
        bclconvert_report_library_path=BCLCONVERT_REPORT_LIBRARY,
        bclconvert_reports_path=bclconvert_reports_path)
    report_dest_path = \
      os.path.join(
        temp_dir,
        os.path.basename(report_file))
    copy_local_file(
      report_file,
      report_dest_path)
    # add report to xcom
    ti.xcom_push(
      key=demult_report_key,
      value=report_dest_path)
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
    ti = context.get('ti')
    dag_run = context.get('dag_run')
    demult_dir_key = \
      context['params'].\
      get(
        'demult_dir_key',
        'demult_dir')
    demult_info_key = \
      context['params'].\
      get(
        'demult_info_key',
        'demult_info')
    samplesheet_index = \
      context['params'].\
      get('samplesheet_index')
    index_column = \
      context['params'].\
      get('index_column', 'index')
    mod_samplesheet_xcom_key = \
      context['params'].\
      get(
        'mod_samplesheet_xcom_key',
        'mod_samplesheet')
    mod_samplesheet_xcom_task = \
      context['params'].\
      get('mod_samplesheet_xcom_task')
    samplesheet_file = \
      ti.xcom_pull(
        task_ids=mod_samplesheet_xcom_task,
        key=mod_samplesheet_xcom_key)
    check_file_path(samplesheet_file)
    seqrun_id = None
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('seqrun_id') is not None:
      seqrun_id = \
        dag_run.conf.get('seqrun_id')
    if seqrun_id is None:
      raise ValueError('seqrun_id is not found in dag_run.conf')
    # seqrun path
    seqrun_path = \
      os.path.join(HPC_SEQRUN_BASE_PATH, seqrun_id)
    temp_dir = \
      get_temp_dir(use_ephemeral_space=True)
    demult_dir = \
      os.path.join(
        temp_dir,
        'demult')
    cmd = \
      bclconvert_singularity_wrapper(
        image_path=BCLCONVERT_IMAGE,
        input_dir=seqrun_path,
        output_dir=demult_dir,
        samplesheet_file=samplesheet_file,
        bcl_num_conversion_threads=1,
        bcl_num_compression_threads=1,
        bcl_num_decompression_threads=1,
        bcl_num_parallel_tiles=1,
        first_tile_only=True)
    check_file_path(
      os.path.join(
        demult_dir,
        'Reports',
        'Demultiplex_Stats.csv'))
    ti.xcom_push(
      key=demult_dir_key,
      value=demult_dir)
    demult_info = {
      index_column: samplesheet_index,
      'demult_dir': demult_dir}
    ti.xcom_push(
      key=demult_info_key,
      value=demult_info)
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


def calculate_override_bases_mask_func(**context):
  try:
    ti = context.get('ti')
    dag_run = context.get('dag_run')
    mod_samplesheet_xcom_key = \
      context['params'].\
      get(
        'mod_samplesheet_xcom_key',
        'mod_samplesheet')
    formatted_samplesheet_xcom_key = \
      context['params'].\
      get(
        'formatted_samplesheet_xcom_key',
        'formatted_samplesheet_data')
    formatted_samplesheet_xcom_task = \
      context['params'].\
      get(
        'formatted_samplesheet_xcom_task',
        'get_formatted_samplesheets')
    samplesheet_index = \
      context['params'].\
      get('samplesheet_index')
    index_column = \
      context['params'].\
      get('index_column', 'index')
    samplesheet_file_column = \
      context['params'].\
      get('samplesheet_file_column', 'samplesheet_file')
    formatted_samplesheet_data = \
      ti.xcom_pull(
        task_ids=formatted_samplesheet_xcom_task,
        key=formatted_samplesheet_xcom_key)
    if not isinstance(formatted_samplesheet_data, list) or \
       len(formatted_samplesheet_data)==0:
      raise ValueError('formatted_samplesheet_data is empty')
    df = pd.DataFrame(formatted_samplesheet_data)
    df[index_column] = \
      df[index_column].\
        astype(str)
    filtered_df = \
      df[df[index_column]==str(samplesheet_index)]
    if len(filtered_df)==0:
      raise ValueError('No samplesheet index found in the samplesheet')
    if samplesheet_file_column not in filtered_df.columns:
      raise ValueError('samplesheet_file column not found in the samplesheet df')
    # get samplesheet
    samplesheet_file_path = \
      filtered_df[samplesheet_file_column].values[0]
    check_file_path(samplesheet_file_path)
    # fetch seqrun id and override cycle
    seqrun_id = None
    override_cycles = ''
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('seqrun_id') is not None:
      seqrun_id = \
        dag_run.conf.get('seqrun_id')
      if 'override_cycles' in dag_run.conf and \
         dag_run.conf.get('override_cycles') is not None and \
         dag_run.conf.get('override_cycles') != '':
        override_cycles = \
          dag_run.conf.get('override_cycles')
    if seqrun_id is None:
      raise ValueError('seqrun_id is not found in dag_run.conf')
    # seqrun path
    seqrun_path = \
      os.path.join(HPC_SEQRUN_BASE_PATH, seqrun_id)
    # runinfo path
    runinfo_path = \
      os.path.join(seqrun_path, 'RunInfo.xml')
    # calculate bases mask
    if override_cycles == '':                           # only calculate if override cycle is not present
      override_cycles = \
        _calculate_bases_mask(
          samplesheet_file=samplesheet_file_path,
          runinfoxml_file=runinfo_path)
    # write new samplesheet
    temp_dir = \
      get_temp_dir(use_ephemeral_space=True)
    new_samplesheet_path = \
      os.path.join(
        temp_dir,
        os.path.basename(samplesheet_file_path))
    mod_sa = SampleSheet(samplesheet_file_path)
    mod_sa.\
      set_header_for_bclconvert_run(
        bases_mask=override_cycles)
    mod_sa.\
      print_sampleSheet(new_samplesheet_path)
    # add new samplesheet to xcom
    ti.xcom_push(
      key=mod_samplesheet_xcom_key,
      value=new_samplesheet_path)
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
  output_dir: str,
  singlecell_tag: str = '10X',
  index_column: str = 'index',
  index2_column: str = 'index2',
  lane_column: str = 'Lane',
  description_column: str = 'Description',
  index2_rule: str = 'NO_CHANGE') -> list:
  try:
    check_file_path(samplesheet_file)
    check_file_path(singlecell_barcode_json)
    check_file_path(singlecell_dual_barcode_json)
    check_file_path(output_dir)
    tmp_dir = get_temp_dir()
    tmp_samplesheet1 = \
      os.path.join(
        tmp_dir,
        'temp1_samplesheet.csv')
    sc_dual_process = \
      ProcessSingleCellDualIndexSamplesheet(
        samplesheet_file=samplesheet_file,
        singlecell_dual_index_barcode_json=singlecell_dual_barcode_json,
        platform=platform,
        index2_rule=index2_rule,
        singlecell_tag=singlecell_tag)
    sc_dual_process.\
      modify_samplesheet_for_sc_dual_barcode(
        output_samplesheet=tmp_samplesheet1)
    tmp_samplesheet2 = \
      os.path.join(
        tmp_dir,
        'temp2_samplesheet.csv')
    sc_data = \
      ProcessSingleCellSamplesheet(
        samplesheet_file=tmp_samplesheet1,
        singlecell_barcode_json=singlecell_barcode_json,
        singlecell_tag=singlecell_tag)
    sc_data.\
      change_singlecell_barcodes(
        tmp_samplesheet2)
    # read samplesheet data
    sa = SampleSheet(tmp_samplesheet2)
    df = pd.DataFrame(sa._data)
    # convert index2 based on rules
    if index2_rule == 'REVCOMP':
      df[index2_column] = \
        pd.np.where(
          df[description_column]!=singlecell_tag,
          df[index2_column].map(lambda x: rev_comp(x)),
          df[index2_column])
    # add index length column
    df[index_column] = df[index_column].fillna('')
    index_column_list = ['index']
    if index2_column in df.columns:
      df[index2_column] = df[index2_column].fillna('')
      index_column_list.append('index2')
    df['index_length'] = \
      df[index_column_list].\
      agg(''.join, axis=1).\
      map(lambda x: len(x.replace(' ', '')))
    # group data per lane per index group
    counter = 0
    formatted_samplesheets = list()
    # group data per lane and per index group
    if lane_column in df.columns:
      for (lane, index_length), lane_df in df.groupby([lane_column, 'index_length']):
        samplesheet_file = \
          os.path.join(
            output_dir,
            f'SampleSheet_{lane}_{index_length}.csv')
        sa._data = \
          lane_df[sa._data_header].\
          to_dict(orient='records')
        sa.print_sampleSheet(samplesheet_file)
        counter += 1
        formatted_samplesheets.\
          append({
            'index': counter,
            'lane': lane,
            'tag': f'{lane}_{index_length}',
            'samplesheet_file': samplesheet_file})
      # merged samplesheet per lane
      for lane, lane_df in df.groupby(lane_column):
        min_index1 = \
          lane_df[index_column].\
            map(lambda x: len(x)).min()
        lane_df.loc[:, index_column] = \
          lane_df[index_column].\
            map(lambda x: x[0:- min_index1])
        if index2_column in lane_df.columns:
          min_index2 = \
            lane_df[index2_column].\
              map(lambda x: len(x)).min()
          lane_df.loc[:, index2_column] = \
            lane_df[index2_column].\
              map(lambda x: x[0: min_index2])
          lane_df.loc[:,'c_index'] = \
            lane_df[index_column] + lane_df[index2_column]
        else:
          lane_df.loc[:,'c_index'] = lane_df[index_column]
        lane_df.\
          drop_duplicates(
            'c_index',
            inplace=True)
        samplesheet_file = \
          os.path.join(
            output_dir,
            f'SampleSheet_{lane}.csv')
        sa._data = \
          lane_df[sa._data_header].\
          to_dict(orient='records')
        sa.print_sampleSheet(samplesheet_file)
        counter += 1
        formatted_samplesheets.\
          append({
            'index': counter,
            'lane': lane,
            'tag': f'merged',
            'samplesheet_file': samplesheet_file})
    else:
      # group data per index group
      for index_length, lane_df in df.groupby('index_length'):
        samplesheet_file = \
          os.path.join(
            output_dir,
            f'SampleSheet_{index_length}.csv')
        sa._data = \
          lane_df[sa._data_header].\
          to_dict(orient='records')
        sa.print_sampleSheet(samplesheet_file)
        counter += 1
        formatted_samplesheets.\
          append({
            'index': counter,
            'lane': 'all',
            'tag': index_length,
            'samplesheet_file': samplesheet_file})
      # merged samplesheet
      min_index1 = \
        df[index_column].\
        map(lambda x: len(x)).min()
      df.loc[:, index_column] = \
        df[index_column].\
          map(lambda x: x[0:- min_index1])
      if index2_column in df.columns:
        min_index2 = \
          df[index2_column].\
            map(lambda x: len(x)).min()
        df.loc[:, index2_column] = \
          df['index2'].\
            map(lambda x: x[0: min_index2])
        df.loc[:,'c_index'] = \
          df[index_column] + df[index2_column]
      else:
        df.loc[:,'c_index'] = df[index_column]
      df.\
        drop_duplicates(
          'c_index',
          inplace=True)
      samplesheet_file = \
        os.path.join(
          output_dir,
          'SampleSheet_merged.csv')
      sa._data = \
        df[sa._data_header].\
        to_dict(orient='records')
      sa.print_sampleSheet(samplesheet_file)
      counter += 1
      formatted_samplesheets.\
        append({
          'index': counter,
          'lane': 'all',
          'tag': 'merged',
          'samplesheet_file': samplesheet_file})
    return formatted_samplesheets
  except Exception as e:
    raise ValueError(f"Failed to format samplesheet per index group, error: {e}")


def get_formatted_samplesheets_func(**context):
  try:
    ti = context.get('ti')
    dag_run = context.get('dag_run')
    samplesheet_xcom_key = \
      context['params'].get('samplesheet_xcom_key', 'samplesheet_data')
    samplesheet_xcom_task = \
      context['params'].get('samplesheet_xcom_task', 'get_samplesheet_from_portal')
    formatted_samplesheet_xcom_key = \
      context['params'].get('formatted_samplesheet_xcom_key', 'formatted_samplesheet_data')
    samplesheet_tag_label = \
      context['params'].get('samplesheet_tag_label', 'samplesheet_tag')
    samplesheet_file = \
      context['params'].get('samplesheet_file', 'samplesheet_file')
    next_task_prefix = \
      context['params'].get('next_task_prefix', 'bcl_convert_run_')
    singlecell_tag = \
      context['params'].get('singlecell_tag', '10X')
    samplesheet_data = \
      ti.xcom_pull(
        task_ids=samplesheet_xcom_task,
        key=samplesheet_xcom_key)
    if not isinstance(samplesheet_data, dict) or \
       samplesheet_tag_label not in samplesheet_data or \
       samplesheet_file not in samplesheet_data:
      raise ValueError(
        'samplesheet_data is not in the correct format')
    #samplesheet_tag_name = \
    #  samplesheet_data.get(samplesheet_tag)
    samplesheet_file_path = \
      samplesheet_data.get(samplesheet_file)
    seqrun_id = None
    #override_cycles = ''
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('seqrun_id') is not None:
      seqrun_id = \
        dag_run.conf.get('seqrun_id')
      # don't need override now
      #if 'override_cycles' in dag_run.conf:
      #  override_cycles = \
      #    dag_run.conf.get('override_cycles')
    if seqrun_id is None:
      raise ValueError('seqrun_id is not found in dag_run.conf')
    # TO Do following
    # * get index 2 rule from db
    # Seqrun is present on seqrun and seqrun_attribute table
    # and has attribute_nale 'flowcell' which matches the Flowcell_barcode_rule.flowcell_type
    db_params = \
      read_dbconf_json(DATABASE_CONFIG_FILE)
    seqrun_adp = \
      SeqrunAdaptor(**db_params)
    seqrun_adp.start_session()
    platform = \
      seqrun_adp.\
        fetch_platform_info_for_seqrun(
          seqrun_igf_id=seqrun_id)
    flowcell_rule = \
      seqrun_adp.\
        fetch_flowcell_barcode_rules_for_seqrun(
          seqrun_igf_id=seqrun_id,
          flowcell_label='flowcell',
          output_mode='dataframe')
    index2_rule = \
      flowcell_rule['index_2'].values[0]
    seqrun_adp.close_session()
    if len(flowcell_rule.index) == 0:
      raise ValueError(
        f"No flowcell barcode rule found for seqrun {seqrun_id}")
    # * split samplesheet per index group
    output_dir = \
      get_temp_dir(use_ephemeral_space=True)
    formatted_samplesheets = \
      _format_samplesheet_per_index_group(
        samplesheet_file=samplesheet_file_path,
        singlecell_barcode_json=SINGLECELL_BARCODE_JSON,
        singlecell_dual_barcode_json=SINGLECELL_DUAL_BARCODE_JSON,
        platform=platform,
        singlecell_tag=singlecell_tag,
        output_dir=output_dir,
        index2_rule=index2_rule)
    if len(formatted_samplesheets) == 0:
      raise ValueError(
        f"No samplesheet found for seqrun {seqrun_id}")
    ti.xcom_push(
      key=formatted_samplesheet_xcom_key,
      value=formatted_samplesheets)
    formatted_samplesheets_df = \
      pd.DataFrame(formatted_samplesheets)
    task_list = [
      f'{next_task_prefix}{si}'
        for si in formatted_samplesheets_df['index'].values.tolist()]
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