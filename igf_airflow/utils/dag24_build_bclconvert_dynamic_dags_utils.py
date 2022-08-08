import os
import stat
import json
import logging
import pandas as pd
from typing import Tuple, Any
from airflow.models import Variable
from airflow.models import taskinstance
from igf_portal.api_utils import upload_files_to_portal
from igf_portal.api_utils import get_data_from_portal
from igf_data.utils.fileutils import copy_local_file
from igf_data.utils.fileutils import check_file_path
from igf_data.utils.fileutils import copy_remote_file
from igf_data.utils.fileutils import get_temp_dir
from igf_data.illumina.runinfo_xml import RunInfo_xml
from igf_data.utils.pipelineutils import check_and_load_pipeline
from igf_airflow.utils.dag22_bclconvert_demult_utils import _get_formatted_samplesheets
from igf_airflow.utils.dag22_bclconvert_demult_utils import _create_output_from_jinja_template
from igf_data.utils.dbutils import read_dbconf_json
from igf_airflow.logging.upload_log_msg import send_log_to_channels

log = logging.getLogger(__name__)

IGF_PORTAL_CONF = Variable.get('igf_portal_conf', default_var=None)
SLACK_CONF = Variable.get('slack_conf',default_var=None)
MS_TEAMS_CONF = Variable.get('ms_teams_conf',default_var=None)
HPC_SEQRUN_BASE_PATH = Variable.get('hpc_seqrun_path', default_var=None)
DATABASE_CONFIG_FILE = Variable.get('database_config_file', default_var=None)
SINGLECELL_BARCODE_JSON = Variable.get('singlecell_barcode_json', default_var=None)
SINGLECELL_DUAL_BARCODE_JSON = Variable.get('singlecell_dual_barcode_json', default_var=None)


def _fetch_samplesheet_for_run(
      portal_conf: str,
      seqrun_id: str,
      override_cycles_key: str = 'override_cycle',
      samplesheet_id_key: str = 'samplesheet_id') \
        -> Tuple[str, str, str, int]:
  try:
    temp_dir = \
      get_temp_dir(use_ephemeral_space=True)
    seqrun_id_json = \
      os.path.join(temp_dir, 'seqrun_id.json')
    with open(seqrun_id_json, 'w') as fp:
      json.dump({'seqrun_id': seqrun_id}, fp)
    ## fetch samplesheet data
    res = \
      upload_files_to_portal(
        url_suffix="/api/v1/raw_seqrun/search_run_samplesheet",
        portal_config_file=portal_conf,
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
    ## fetch override cycles
    res = \
      get_data_from_portal(
        url_suffix=f"/api/v1/raw_seqrun/get_run_override_cycle/{seqrun_id}",
        portal_config_file=portal_conf,
        request_mode='post',
        verify=False,
        jsonify=False)
    if res.status_code != 200:
      raise ValueError('Failed to get override cycls from portal')
    data = res.content.decode('utf-8')
    json_data = json.loads(data)
    if override_cycles_key not in json_data:
      raise KeyError(f'Missing key {override_cycles_key}')
    override_cycles = json_data.get(override_cycles_key)
    ## fetch samplesheet id
    res = \
      get_data_from_portal(
        url_suffix=f"/api/v1/raw_seqrun/get_samplesheet_id/{seqrun_id}",
        portal_config_file=portal_conf,
        request_mode='post',
        verify=False,
        jsonify=False)
    if res.status_code != 200:
      raise ValueError('Failed to get samplesheet ids from portal')
    data = res.content.decode('utf-8')
    json_data = json.loads(data)
    if samplesheet_id_key not in json_data:
      raise KeyError(f'Missing key {samplesheet_id_key}')
    samplesheet_id = json_data.get(samplesheet_id_key)
    return samplesheet_file, samplesheet_tag, override_cycles, samplesheet_id
  except Exception as e:
    raise ValueError(
      f'Failed to get samplesheet for seqrun_id: {seqrun_id}, error: {e}')


def fetch_seqrun_data_from_portal_func(**context):
  try:
    ti = context.get('ti')
    samplesheet_info_key = \
      context['params'].\
        get('samplesheet_info_key', 'samplesheet_info')
    dag_run = context.get('dag_run')
    # get seqrun id
    seqrun_id = None
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('seqrun_id') is not None:
      seqrun_id = dag_run.conf.get('seqrun_id')
    if seqrun_id is None:
      raise ValueError('seqrun_id is not provided')
    ## get samplesheet info from portal
    samplesheet_file, samplesheet_tag, override_cycles, samplesheet_id = \
      _fetch_samplesheet_for_run(
        portal_conf=IGF_PORTAL_CONF,
        seqrun_id=seqrun_id)
    ## add samplesheet to xcom
    xcom_data = {
      'samplesheet_file': samplesheet_file,
      'samplesheet_tag': samplesheet_tag,
      'override_cycles': override_cycles,
      'samplesheet_id': samplesheet_id}
    ti.xcom_push(
      key=samplesheet_info_key,
      value=xcom_data)
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


def format_samplesheet_func(**context):
  try:
    ti = context.get('ti')
    dag_run = context.get('dag_run')
    samplesheet_info_key = \
      context['params'].\
        get('samplesheet_info_key', 'samplesheet_info')
    samplesheet_info_task = \
      context['params'].\
        get('samplesheet_info_task', 'fetch_seqrun_data_from_portal')
    samplesheet_file_key = \
      context['params'].\
        get('samplesheet_file_key', 'samplesheet_file')
    override_cycles_key = \
      context['params'].\
        get('override_cycles_key', 'override_cycles')
    tenx_sc_tag = \
      context['params'].\
        get('tenx_sc_tag', '10X')
    run_info_filname = \
      context['params'].\
        get('run_info_filname', 'RunInfo.xml')
    formatted_samplesheets_key = \
      context['params'].\
        get('formatted_samplesheets_key', 'formatted_samplesheets')
    sample_groups_key = \
      context['params'].\
        get('sample_groups_key', 'sample_groups')
    # get seqrun id
    seqrun_id = None
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('seqrun_id') is not None:
      seqrun_id = dag_run.conf.get('seqrun_id')
    if seqrun_id is None:
      raise ValueError('seqrun_id is not provided')
    # fetch samplesheet info
    samplesheet_info = \
      ti.xcom_pull(
        task_ids=samplesheet_info_task,
        key=samplesheet_info_key)
    if samplesheet_info is None:
      raise ValueError(
        f'Failed to get samplesheet info from xcom')
    if samplesheet_file_key not in samplesheet_info:
      raise ValueError(
        f'Failed to get samplesheet file from xcom')
    samplesheet_file = \
      samplesheet_info.get(samplesheet_file_key)
    override_cycles = \
      samplesheet_info.get(override_cycles_key)
    if override_cycles is None:
      override_cycles = ''
    # get runinfor path
    run_info_xml = \
      os.path.join(
        HPC_SEQRUN_BASE_PATH,
        seqrun_id,
        run_info_filname)
    check_file_path(run_info_xml)
    # get formatte samplesheets for pipeline
    formatted_samplesheet_dir = \
      get_temp_dir(use_ephemeral_space=True)
    samplesheets = \
      _get_formatted_samplesheets(
        samplesheet_file=samplesheet_file,
        runinfo_xml_file=run_info_xml,
        samplesheet_output_dir=formatted_samplesheet_dir,
        singlecell_barcode_json=SINGLECELL_BARCODE_JSON,
        singlecell_dual_barcode_json=SINGLECELL_DUAL_BARCODE_JSON,
        tenx_sc_tag=tenx_sc_tag,
        override_cycles=override_cycles)
    sample_groups = \
      _get_sample_group_info_for_formatted_samplesheets(
        samplesheets=samplesheets)
    # push to xcom
    ti.xcom_push(
      key=formatted_samplesheets_key,
      value=samplesheets)
    ti.xcom_push(
      key=sample_groups_key,
      value=sample_groups)
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


def _get_sample_group_info_for_formatted_samplesheets(
      samplesheets: list,
      project_index_key: str = 'project_index',
      lane_index_key: str = "lane_index",
      index_group_index_key: str = "index_group_index") \
        -> dict:
  try:
    df = pd.DataFrame(samplesheets)
    sample_groups = dict()
    for project_index, p_data in df.groupby(project_index_key):
      project_level_group = dict()
      for lane_index, l_data in p_data.groupby(lane_index_key):
        lane_level_group = dict()
        for index_group_index, i_data in l_data.groupby(index_group_index_key):
          sample_counts = \
            i_data['sample_counts'].values.tolist()[0]
          lane_level_group.\
            update({
              index_group_index: sample_counts})
        project_level_group.\
          update({
            lane_index: lane_level_group})
      sample_groups.\
        update({
          project_index: project_level_group})
    return sample_groups
  except Exception as e:
    raise ValueError(
      f'Failed to get sample group info: {e}')