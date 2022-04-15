from csv import excel_tab
from tabnanny import check
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
from igf_data.utils.fileutils import get_date_stamp
from igf_data.utils.fileutils import get_date_stamp_for_file_name
from igf_data.utils.singularity_run_wrapper import execute_singuarity_cmd
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.utils.singularity_run_wrapper import execute_singuarity_cmd
from igf_data.utils.jupyter_nbconvert_wrapper import Notebook_runner


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

log = logging.getLogger(__name__)

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
        singularity_options=['--no-home','-C', "--env", "PYTHONPATH={0}".format(bclconvert_report_library_path)],
        allow_errors=False,
        singularity_image_path=image_path,
        dry_run=dry_run)
    output_notebook_path, _ = \
      nb.execute_notebook_in_singularity()
    return output_notebook_path
  except Exception as e:
    raise ValueError(
            "Failed to generate bclconvert report, error: {0}".format(e))


def bclconvert_report_func(**context):
  try:
    ti = context['ti']
    xcom_key_for_reports = \
      context['params'].get('xcom_key_for_reports', 'bclconvert_reports')
    xcom_task_for_reports = \
      context['params'].get('xcom_task_for_reports', None)
    bclconvert_reports_path = \
      ti.xcom_pull(
        key=xcom_key_for_reports,
        task_ids=xcom_task_for_reports)
    report_file = \
      generate_bclconvert_report(
        seqrun_path=None,
        image_path=INTEROP_NOTEBOOK_IMAGE,
        report_template=BCLCONVERT_REPORT_TEMPLATE,
        bclconvert_report_library_path=BCLCONVERT_REPORT_LIBRARY,
        bclconvert_reports_path=bclconvert_reports_path)
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
      dry_run: bool = False) \
      -> str:
  try:
    check_file_path(image_path)
    check_file_path(input_dir)
    check_file_path(output_dir)
    check_file_path(samplesheet_file)
    temp_dir = get_temp_dir()
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
    if lane_id > 0:
      bclconvert_cmd.extend(["--bcl-only-lane", str(lane_id)])
    if len(tile_id_list) > 0:
      bclconvert_cmd.extend(["--tiles", ",".join(tile_id_list)])
    bclconvert_cmd = \
      ' '.join(bclconvert_cmd)
    bind_paths = [
      '{0}:/var/log'.format(temp_dir),
      os.path.dirname(samplesheet_file),
      input_dir,
      output_dir]
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
    xcom_key = \
      context['params'].get('xcom_key', 'formatted_samplesheets')
    xcom_task = \
      context['params'].get('xcom_task', 'format_and_split_samplesheet')
    project_index_column = \
      context['params'].get('project_index_column', 'project_index')
    project_index = \
      context['params'].get('project_index', 0)
    project_column = \
      context['params'].get('project_column', 'project')
    lane_index_column = \
      context['params'].get('lane_index_column', 'lane_index')
    lane_column = \
      context['params'].get('lane_column', 'lane')
    lane_index = \
      context['params'].get('lane_index', 0)
    ig_index_column = \
      context['params'].get('ig_index_column', 'index_group_index')
    index_group_column = \
      context['params'].get('index_group_column', 'index_group')
    ig_index = \
      context['params'].get('ig_index', 0)
    samplesheet_column = \
      context['params'].get('samplesheet_column', 'samplesheet_file')
    xcom_key_for_reports = \
      context['params'].get('xcom_key_for_reports', 'bclconvert_reports')
    bcl_num_conversion_threads = \
      context['params'].get('bcl_num_conversion_threads', '1')
    bcl_num_compression_threads = \
      context['params'].get('bcl_num_compression_threads', '1')
    bcl_num_decompression_threads = \
      context['params'].get('bcl_num_decompression_threads', '1')
    bcl_num_parallel_tiles = \
      context['params'].get('bcl_num_parallel_tiles', '1')
    dag_run = context.get('dag_run')
    seqrun_path = ''
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('seqrun_id') is not None:
      seqrun_id = \
        dag_run.conf.get('seqrun_id')
      seqrun_path = \
        os.path.join(HPC_SEQRUN_BASE_PATH, seqrun_id)
    else:
      raise IOError("Failed to get seqrun_id from dag_run")
    if project_index == 0 or \
       lane_index == 0 or \
       ig_index == 0:
      raise ValueError('project_index, lane_index or ig_index is not set')
    if xcom_key is None or \
       xcom_task is None:
      raise ValueError('xcom_key or xcom_task is not set')
    formatted_samplesheets_list = \
      ti.xcom_pull(task_ids=xcom_task, key=xcom_key)
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
          "No samplesheet found for project {0}, lane {1}, ig {2}".\
          format(project_index, lane_index, ig_index))
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
        '{0}_{1}_{2}'.format(project_id, lane_id, ig_id))
    if not os.path.exists(demult_dir):
      os.makedirs(demult_dir)
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
    copy_local_file(
      demult_dir,
      os.path.join(
        output_dir,
        '{0}_{1}_{2}'.format(
          project_id,
          lane_id,
          ig_id)))
    reports_dir = \
      os.path.join(
        output_dir,
        '{0}_{1}_{2}'.format(
          project_id,
          lane_id,
          ig_id),
        'Reports')
    check_file_path(reports_dir)
    ti.xcom_push(
      key=xcom_key_for_reports,
      value=reports_dir)
    message = \
      'Finished demultiplexing project {0}, lane {1}, ig {2} - cmd: {3}'.\
      format(project_id, lane_id, ig_id, cmd)
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
    xcom_key = \
      context['params'].get('xcom_key', 'formatted_samplesheets')
    xcom_task = \
      context['params'].get('xcom_task', 'format_and_split_samplesheet')
    project_index_column = \
      context['params'].get('project_index_column', 'project')
    project_index = \
      context['params'].get('project_index', 0)
    lane_index_column = \
      context['params'].get('lane_index_column', 'lane')
    lane_index = \
      context['params'].get('lane_index', 0)
    ig_task_prefix = \
      context['params'].get('ig_task_prefix')
    max_index_groups = \
      context['params'].get('max_index_groups')
    ig_index_column = \
      context['params'].get('ig_index_column', 'index_group_index')
    formatted_samplesheets_list = \
      ti.xcom_pull(task_ids=xcom_task, key=xcom_key)
    if len(formatted_samplesheets_list) == 0:
      raise ValueError(
              "No samplesheet found for seqrun {0}".\
              format(context['dag_run'].conf.get('seqrun_id')))
    df = pd.DataFrame(formatted_samplesheets_list)
    if project_index == 0 :
      raise ValueError("Invalid projext index 0")
    if project_index_column not in df.columns:
      raise KeyError("Column {0} not found in samplesheet".\
                     format(project_index_column))
    if lane_index == 0 :
      raise ValueError("Invalid lane index 0")
    if lane_index_column not in df.columns:
      raise KeyError("Column {0} not found in samplesheet".\
                      format(lane_index_column))
    if ig_index_column not in df.columns:
      raise KeyError("Column {0} not found in samplesheet".\
                      format(ig_index_column))
    df[project_index_column] = df[project_index_column].astype(int)
    df[lane_index_column] = df[lane_index_column].astype(int)
    df[ig_index_column] = df[ig_index_column].astype(int)
    project_df = df[df[project_index_column] == int(project_index)]
    lane_df = project_df[project_df[lane_index_column] == int(lane_index)]
    if len(lane_df.index) == 0:
      raise ValueError("No samplesheet found for project {0}, lane {1}".\
                       format(project_index, lane_index))
    ig_counts = \
      lane_df[ig_index_column].\
      drop_duplicates().\
      values.\
      tolist()
    if len(ig_counts) == 0:
      raise ValueError("No index group found for project {0}, lane {1}".\
                       format(project_index, lane_index))
    if len(ig_counts) > max_index_groups:
      raise ValueError("Too many index groups found for project {0}, lane {1}".\
                       format(project_index, lane_index))
    task_list = [
      '{0}_{1}'.format(ig_task_prefix, ig)
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
    xcom_key = \
      context['params'].get('xcom_key', 'formatted_samplesheets')
    xcom_task = \
      context['params'].get('xcom_task', 'format_and_split_samplesheet')
    project_index_column = \
      context['params'].get('project_index_column', 'project')
    project_index = \
      context['params'].get('project_index', 0)
    lane_index_column = \
      context['params'].get('lane_index_column', 'lane')
    lane_task_prefix = \
      context['params'].get('lane_task_prefix')
    max_lanes = \
      context['params'].get('max_lanes', 0)
    formatted_samplesheets_list = \
      ti.xcom_pull(task_ids=xcom_task, key=xcom_key)
    if len(formatted_samplesheets_list) == 0:
      raise ValueError(
              "No samplesheet found for seqrun {0}".\
              format(context['dag_run'].conf.get('seqrun_id')))
    df = pd.DataFrame(formatted_samplesheets_list)
    if project_index == 0 :
      raise ValueError("Invalid projext index 0")
    if project_index_column not in df.columns:
      raise KeyError("Column {0} not found in samplesheet".\
                     format(project_index_column))
    if lane_index_column not in df.columns:
      raise KeyError("Column {0} not found in samplesheet".\
                      format(lane_index_column))
    df[project_index_column] = df[project_index_column].astype(int)
    project_df = df[df[project_index_column] == int(project_index)]
    lane_counts = \
      project_df[lane_index_column].\
      drop_duplicates().\
      values.tolist()
    if len(lane_counts) == 0:
      raise ValueError("No lane found for project {0}".\
                      format(project_index))
    if len(lane_counts) > max_lanes:
      raise ValueError("Too many lanes {0} found for project {1}".\
                      format(lane_counts, project_index))
    task_list = [
      '{0}_{1}'.format(lane_task_prefix, lane_count)
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


def format_and_split_samplesheet_func(**context):
  try:
    ti = context['ti']
    xcom_key = \
      context['params'].get('xcom_key', 'formatted_samplesheets')
    max_projects = \
      context['params'].get('max_projects', 0)
    project_task_prefix = \
      context['params'].get('project_task_prefix', 'demult_start_project_')
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
      ti.xcom_push(
        key=xcom_key,
        value=formatted_samplesheets_list)
      project_indices = \
        pd.DataFrame(formatted_samplesheets_list)['project_index'].\
        drop_duplicates().values.tolist()
      if len(project_indices) > max_projects:
        raise ValueError(
                "Too many projects {0}. Increase MAX_PROJECTS param from {1}".\
                format(project_indices, max_projects))
      task_list = [
        '{0}{1}'.format(project_task_prefix,project_index)
          for project_index in project_indices]
      if len(task_list) == 0:
        log.warning(
          "No project indices found in samplesheet {0}".\
            format(samplesheet_file))
        task_list = ['mark_run_finished']
    else:
      log.warning("No seqrun_id found in dag_run conf")
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
              formatted_samplesheets_list.\
                append({
                  'project': project_name,
                  'project_index': project_counter,
                  'lane': lane_id,
                  'lane_index': lane_counter,
                  'bases_mask': bases_mask,
                  'index_group': '{0}_{1}'.format(ig, desc_item),
                  'index_group_index': ig_counter,
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
                'samplesheet_file': ig_samplesheet_path,
                'output_dir': temp_dir})
    return formatted_samplesheets_list
  except Exception as e:
    raise ValueError(
            "Failed to get formatted samplesheets and bases mask, error: {0}".\
            format(e))


def _calculate_bases_mask(
  samplesheet_file: str,
  runinfoxml_file: str,
  numcycle_label: str='numcycles',
  isindexedread_label: str='isindexedread',
  read_offset: int=1,
  read_offset_cutoff: int=50) -> str:
  try:
    samplesheet_data = SampleSheet(infile=samplesheet_file)
    index_length_stats = samplesheet_data.get_index_count()
    samplesheet_index_length_list = list()
    for index_name in index_length_stats.keys():
      index_type = len(index_length_stats.get(index_name).keys())
      if index_type > 1:
        raise ValueError('column {0} has variable lengths'.format( index_type ))
      index_length = list(index_length_stats.get(index_name).keys())[0]
      samplesheet_index_length_list.\
        append(index_length)
    runinfo_data = RunInfo_xml(xml_file=runinfoxml_file)
    runinfo_reads_stats = runinfo_data.get_reads_stats()
    for read_id in (sorted(runinfo_reads_stats.keys())):
      runinfo_read_length = int(runinfo_reads_stats[read_id][numcycle_label])
      if runinfo_reads_stats[read_id][isindexedread_label] == 'N':
        if int(runinfo_read_length) < read_offset_cutoff:
          read_offset = 0
    index_read_position = 0
    bases_mask_list = list()
    for read_id in (sorted(runinfo_reads_stats.keys())):
      runinfo_read_length = int(runinfo_reads_stats[read_id][numcycle_label])
      if runinfo_reads_stats[read_id][isindexedread_label] == 'Y':
        samplesheet_index_length = \
          samplesheet_index_length_list[index_read_position]
        index_diff = \
          int(runinfo_read_length) - int(samplesheet_index_length)
        if samplesheet_index_length == 0:
          bases_mask_list.\
            append('N{0}'.format(runinfo_read_length))
          if index_read_position == 0:
            raise ValueError("Index 1 position can't be zero")
        elif index_diff > 0 and \
             samplesheet_index_length > 0:
          bases_mask_list.\
            append('I{0}N{1}'.format(samplesheet_index_length, index_diff))
        elif index_diff == 0:
          bases_mask_list.\
            append('I{0}'.format(samplesheet_index_length, index_diff))
        index_read_position += 1
      else:
        if int(read_offset) > 0:
          bases_mask_list.\
            append('Y{0}N{1}'.format(int(runinfo_read_length) - int(read_offset), read_offset))
        else:
          bases_mask_list.\
            append('Y{0}'.format(runinfo_read_length, read_offset))
    if len(bases_mask_list) < 2:
      raise ValueError("Missing bases mask values")
    return ';'.join(bases_mask_list)
  except:
    raise


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