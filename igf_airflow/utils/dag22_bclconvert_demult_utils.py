from csv import excel_tab
import pandas as pd
from copy import deepcopy
import os, logging, subprocess
from airflow.models import Variable
from igf_data.illumina.runinfo_xml import RunInfo_xml
from igf_data.illumina.runparameters_xml import RunParameter_xml
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.process.singlecell_seqrun.processsinglecellsamplesheet import ProcessSingleCellDualIndexSamplesheet
from igf_data.process.singlecell_seqrun.processsinglecellsamplesheet import ProcessSingleCellSamplesheet
from igf_data.utils.box_upload import upload_file_or_dir_to_box
from igf_airflow.logging.upload_log_msg import send_log_to_channels
from igf_data.utils.fileutils import copy_local_file
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.utils.fileutils import get_temp_dir
from igf_data.utils.fileutils import copy_remote_file
from igf_data.utils.fileutils import check_file_path
from igf_data.utils.fileutils import read_json_data
from igf_data.utils.fileutils import get_date_stamp_for_file_name
from igf_data.utils.singularity_run_wrapper import execute_singuarity_cmd
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.igfdb.baseadaptor import BaseAdaptor


SLACK_CONF = Variable.get('slack_conf',default_var=None)
MS_TEAMS_CONF = Variable.get('ms_teams_conf',default_var=None)
HPC_SEQRUN_BASE_PATH = Variable.get('hpc_seqrun_path', default_var=None)
DATABASE_CONFIG_FILE = Variable.get('database_config_file', default_var=None)
SINGLECELL_BARCODE_JSON = Variable.get('singlecell_barcode_json', default_var=None)
SINGLECELL_DUAL_BARCODE_JSON = Variable.get('singlecell_dual_barcode_json', default_var=None)

log = logging.getLogger(__name__)

def format_and_split_samplesheet_func(**context):
  try:
    dag_run = context.get('dag_run')
    task_list = ['mark_run_finished',]
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('seqrun_id') is not None:
      seqrun_id = \
        dag_run.conf.get('seqrun_id')
      seqrun_path = \
        os.path.join(HPC_SEQRUN_BASE_PATH, seqrun_id)
      samplesheet_file = \
        os.path.join(seqrun_path, 'SampleSheet.csv')
      runinfo_xml_file = \
        os.path.join(seqrun_path, 'RunInfo.xml')
      check_file_path(samplesheet_file)
      samplesheet_dir = \
        get_temp_dir(use_ephemeral_space=True)
      formatted_samplesheets_list = \
        _get_formatted_samplesheets(
          samplesheet_file=samplesheet_file,
          runinfo_xml_file=runinfo_xml_file,
          samplesheet_output_dir=samplesheet_dir,
          singlecell_barcode_json=SINGLECELL_BARCODE_JSON,
          singlecell_dual_barcode_json=SINGLECELL_DUAL_BARCODE_JSON)
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
  tenx_sc_tag: str='10X') -> list:
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
    for project_name, p_data in pd.DataFrame(formatted_project_and_lane).groupby('project_name'):
      for lane_id, _ in p_data.groupby('lane'):
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
              ig_samplesheet_path = \
                os.path.join(
                  samplesheet_output_dir,
                  samplesheet_name)
              ig_sa.\
                print_sampleSheet(ig_samplesheet_path)
              formatted_samplesheets_list.\
                append({
                  'project': project_name,
                  'lane': lane_id,
                  'index_group': '{0}_{1}'.format(ig, desc_item),
                  'samplesheet_file': ig_samplesheet_path})
          else:
            samplesheet_name = \
              'SampleSheet_{0}_{1}_{2}.csv'.\
              format(
                project_name,
                lane_id,
                ig)
            ig_samplesheet_path = \
                os.path.join(
                  samplesheet_output_dir,
                  samplesheet_name)
            ig_sa.\
              print_sampleSheet(ig_samplesheet_path)
            formatted_samplesheets_list.\
              append({
                'project': project_name,
                'lane': lane_id,
                'index_group': ig,
                'samplesheet_file': ig_samplesheet_path})
    return formatted_samplesheets_list
  except Exception as e:
    raise ValueError(
            "Failed to get formatted samplesheets and bases mask, error: {0}".\
            format(e))


def find_seqrun_func(**context):
  try:
    dag_run = context.get('dag_run')
    task_list = ['mark_run_finished',]
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('seqrun_id') is not None:
      seqrun_id = \
        dag_run.conf.get('seqrun_id')
      seqrun_path = \
        os.path.join(HPC_SEQRUN_BASE_PATH, seqrun_id)
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
          seqrun_id=seqrun_id,
          seqrun_path=seqrun_path,
          dbconf_json_path=DATABASE_CONFIG_FILE)
        seed_status = \
          _check_and_seed_seqrun_pipeline(
            seqrun_id=seqrun_id,
            pipeline_name=context['task'].dag_id,
            dbconf_json_path=DATABASE_CONFIG_FILE)
        if seed_status:
          task_list = ['format_and_split_samplesheet',]
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
    raise IOError("Failed to get required files for seqrun {0}".format(seqrun_path))


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
    run_exists = \
      sra.check_seqrun_exists(seqrun_id)
    if not run_exists:
      runinfo_file = os.path.join(seqrun_path, runinfo_file_name)
      runinfo_data = RunInfo_xml(xml_file=runinfo_file)
      platform_name = runinfo_data.get_platform_number()
      flowcell_id = runinfo_data.get_flowcell_name()
      seqrun_data = [{
        'seqrun_igf_id': seqrun_id,
        'platform_igf_id': platform_name,
        'flowcell_id': flowcell_id }]
      sra.store_seqrun_and_attribute_data(
        data=seqrun_data,
        autosave=True)
    sra.close_session()
  except Exception as e:
    raise ValueError(
            "Failed to load seqrun {0} to database, error: {1}".\
            format(seqrun_id, e))


def _check_and_seed_seqrun_pipeline(
    seqrun_id: str,
    pipeline_name: str,
    dbconf_json_path: str,
    seed_status: str = 'SEEDED',
    seed_table: str ='seqrun',
    no_change_status: str = 'RUNNING') -> bool:
  try:
    dbconf = read_dbconf_json(dbconf_json_path)
    base = BaseAdaptor(**dbconf)
    base.start_session()
    sra = SeqrunAdaptor(**{'session': base.session})
    seqrun_entry = \
      sra.fetch_seqrun_records_igf_id(
          seqrun_igf_id=seqrun_id)
    pa = PipelineAdaptor(**{'session': base.session})
    seed_status = \
      pa.create_or_update_pipeline_seed(
        seed_id=seqrun_entry.seqrun_id,
        pipeline_name=pipeline_name,
        new_status=seed_status,
        seed_table=seed_table,
        no_change_status=no_change_status)
    base.close_session()
    return seed_status
  except Exception as e:
    raise ValueError(
            "Faild to seed pipeline {0} for seqrun {1}, error: {2}".\
            format(pipeline_name, seqrun_id, e))