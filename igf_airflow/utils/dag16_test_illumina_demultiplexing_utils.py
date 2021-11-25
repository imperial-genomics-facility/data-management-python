import logging, os
import pandas as pd
from airflow.models import Variable
from igf_airflow.logging.upload_log_msg import send_log_to_channels
from igf_data.utils.fileutils import check_file_path, get_temp_dir, copy_local_file, copy_remote_file
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.utils.samplesheet_utils import get_formatted_samplesheet_per_lane
from igf_data.illumina.runparameters_xml import RunParameter_xml
from igf_data.process.moveBclFilesForDemultiplexing import moveBclTilesForDemultiplexing
from igf_data.utils.tools.bcl2fastq_utils import run_bcl2fastq
from igf_data.utils.box_upload import upload_file_or_dir_to_box
from igf_data.utils.singularity_run_wrapper import singularity_run


SLACK_CONF = Variable.get('slack_conf', default_var=None)
MS_TEAMS_CONF = Variable.get('ms_teams_conf', default_var=None)
HPC_SEQRUN_BASE_PATH = Variable.get('hpc_seqrun_path', default_var=None)
SINGLECELL_BARCODE_JSON = Variable.get('singlecell_barcode_json', default_var=None)
SINGLECELL_DUAL_BARCODE_JSON = Variable.get('singlecell_dual_barcode_json', default_var=None)
BCL2FASTQ_IMAGE = Variable.get('bcl2fastq_image_path', default_var=None)
BOX_DIR_PREFIX = Variable.get('box_dir_prefix_for_seqrun_report', default_var=None)
BOX_USERNAME = Variable.get('box_username', default_var=None)
BOX_CONFIG_FILE  = Variable.get('box_config_file', default_var=None)
INTEROP_IMAGE = Variable.get('interop_notebook_image_path')
SEQRUN_SERVER = Variable.get('seqrun_server', default_var=None)
REMOTE_SEQRUN_BASE_PATH = Variable.get('seqrun_base_path', default_var=None)
SEQRUN_SERVER_USER = Variable.get('seqrun_server_user', default_var=None)


def get_samplesheet_and_decide_flow_func(**context):
  try:
    dag_run = context.get('dag_run')
    ti = context.get('ti')
    samplesheet_xcom_key = \
      context['params'].get('samplesheet_xcom_key')
    demult_task_prefix = \
      context['params'].get('demult_task_prefix')
    runParameters_xml_file_name = \
      context['params'].get('runParameters_xml_file_name')
    runinfo_xml_file_name = \
      context['params'].get('runinfo_xml_file_name')
    output_path_xcom_key = \
      context['params'].get('output_path_xcom_key')
    if dag_run is not None and \
       dag_run.conf is not None:
      seqrun_id = dag_run.conf.get('seqrun_id')
      platform_name = dag_run.conf.get('platform_name')
      index2_rule = dag_run.conf.get('index2_rule')
      samplesheet_file_name = dag_run.conf.get('samplesheet_file_name')
      if seqrun_id is None or \
         samplesheet_file_name is None:
        raise ValueError('Missing samplesheet or seqrun id')
      run_dir = \
        os.path.join(HPC_SEQRUN_BASE_PATH, seqrun_id)
      # checking if run dir is present
      check_file_path(run_dir)
      samplesheet_file = \
        os.path.join(run_dir, samplesheet_file_name)
      # checking if samplesheet is present
      if not os.path.exists(samplesheet_file):
        # copy samplesheet from remote dir
        remote_samplesheet_file = \
          os.path.join(
            REMOTE_SEQRUN_BASE_PATH,
            seqrun_id,
            samplesheet_file_name)
        logging.warn(
          'copying samplesheet {0} from remote server'.\
            format(samplesheet_file_name))
        copy_remote_file(
          source_path=remote_samplesheet_file,
          destination_path=samplesheet_file,
          source_address='{0}@{1}'.format(SEQRUN_SERVER_USER, SEQRUN_SERVER),
          force_update=True)
      check_file_path(samplesheet_file)
      runParameters_path = \
        os.path.join(run_dir, runParameters_xml_file_name)
      check_file_path(runParameters_path)
      runinfo_path = \
        os.path.join(run_dir, runinfo_xml_file_name)
      tmp_samplesheet_dir = \
        get_temp_dir(use_ephemeral_space=True)
      if platform_name is None and \
         index2_rule is None:
        runparameters_data = \
          RunParameter_xml(xml_file=runParameters_path)
        flowcell_type = \
          runparameters_data.\
            get_hiseq_flowcell()
        # check for nova workflow type
        workflow_type = \
          runparameters_data.\
            get_nova_workflow_type()
        if flowcell_type is None and \
           workflow_type is not None:
          flowcell_type = \
            runparameters_data.\
              get_novaseq_flowcell()
          platform_name = 'NOVASEQ6000'
          index2_rule = None
        if flowcell_type is not None and \
           flowcell_type == 'HiSeq 3000/4000 PE':
          index2_rule = 'REVCOMP'
        if flowcell_type is not None and \
           flowcell_type.startswith('HiSeq 3000/4000'):
          platform_name = 'HISEQ4000'
      samplesheet_file_list = \
        get_formatted_samplesheet_per_lane(
          samplesheet_file=samplesheet_file,
          singlecell_barcode_json=SINGLECELL_BARCODE_JSON,
          singlecell_dual_barcode_json=SINGLECELL_DUAL_BARCODE_JSON,
          runinfo_file=runinfo_path,
          output_dir=tmp_samplesheet_dir,
          platform=platform_name,
          single_cell_tag='10X',
          index1_rule=None,
          index2_rule=index2_rule)
      ti.xcom_push(
        key=samplesheet_xcom_key,
        value=samplesheet_file_list)
      tmp_output_path = \
        get_temp_dir(use_ephemeral_space=True)
      ti.xcom_push(
        key=output_path_xcom_key,
        value=tmp_output_path)
      samplesheet = \
        SampleSheet(samplesheet_file)
      task_list = \
        ['{0}_{1}'.format(demult_task_prefix, i)
          for i in samplesheet.get_lane_count()]
      return task_list
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


def run_demultiplexing_func(**context):
  try:
    dag_run = context.get('dag_run')
    ti = context.get('ti')
    samplesheet_xcom_key = \
      context['params'].get('samplesheet_xcom_key')
    samplesheet_xcom_task = \
      context['params'].get('samplesheet_xcom_task')
    output_path_xcom_key = \
      context['params'].get('output_path_xcom_key')
    lane_id = \
      context['params'].get('lane_id')
    runinfo_xml_file_name = \
      context['params'].get('runinfo_xml_file_name')
    threads = \
      context['params'].get('threads')
    if lane_id is None or \
       (lane_id is not None and \
        not isinstance(lane_id, int) and \
        lane_id not in (1, 2, 3, 4, 5, 6, 7, 8)):
      raise ValueError(
              'Incorrect lane_id found: {0}'.format(lane_id))
    if dag_run is not None and \
       dag_run.conf is not None:
      seqrun_id = dag_run.conf.get('seqrun_id')
      samplesheet_file_name = dag_run.conf.get('samplesheet_file_name')
      flowcell_id = dag_run.conf.get('flowcell_id')
      tag_id = dag_run.conf.get('tag_id')
      if seqrun_id is None or \
         flowcell_id is None or \
         tag_id is None or \
         samplesheet_file_name is None:
        raise ValueError('Missing samplesheet, flowcell id, tag_id or seqrun id')
      run_dir = \
        os.path.join(HPC_SEQRUN_BASE_PATH, seqrun_id)
      samplesheet_file = \
        os.path.join(run_dir, samplesheet_file_name)
      samplesheet_list = \
        ti.xcom_pull(
          key=samplesheet_xcom_key,
          task_ids=samplesheet_xcom_task)
      output_path = \
        ti.xcom_pull(
          key=output_path_xcom_key,
          task_ids=samplesheet_xcom_task)
      check_file_path(output_path)
      samplesheet_df = \
        pd.DataFrame(samplesheet_list)
      samplesheet_df['lane_id'] = \
        samplesheet_df['lane_id'].astype(int)
      if len(samplesheet_df[samplesheet_df['lane_id']==int(lane_id)].index) == 0:
        raise ValueError('No records for lane {0} found'.format(lane_id))
      lane_records = \
        samplesheet_df[samplesheet_df['lane_id']==int(lane_id)]
      samplesheet_file = \
        lane_records['samplesheet_file'].values[0]
      bases_mask = \
        lane_records['bases_mask'].values[0]
      if samplesheet_file is None or \
         bases_mask is None:
        raise ValueError(
                'Missing samplesheet or bases_mask for demultiplexing of lane {0}'.\
                  format(lane_id))
      tmp_run_path = \
        get_temp_dir(use_ephemeral_space=False)
      runinfo_path = \
        os.path.join(run_dir, runinfo_xml_file_name)
      move_tiles = \
        moveBclTilesForDemultiplexing(
          input_dir=run_dir,
          output_dir=tmp_run_path,
          samplesheet=samplesheet_file,
          run_info_xml=runinfo_path,
          tiles_list=[1101,])
      tile_list_for_bcl2fq = \
        move_tiles.copy_bcl_files()
      tmp_bcl2fq_output = \
        get_temp_dir(use_ephemeral_space=False)
      tmp_report_path = \
        get_temp_dir(use_ephemeral_space=True)
      _ = \
        run_bcl2fastq(
          runfolder_dir=tmp_run_path,
          output_dir=tmp_bcl2fq_output,
          reports_dir=tmp_report_path,
          samplesheet_path=samplesheet_file,
          bases_mask=bases_mask,
        threads=threads,
        singularity_image_path=BCL2FASTQ_IMAGE,
        tiles=tile_list_for_bcl2fq,
        options=[
          '--barcode-mismatches 1',
          '--auto-set-to-zero-barcode-mismatches',
          '--create-fastq-for-index-reads'])
      box_dir = \
        os.path.join(
          BOX_DIR_PREFIX,
          seqrun_id,
          tag_id,
          'L00{0}'.format(lane_id))
      source_html_file = \
        os.path.join(
          tmp_report_path,
          'html',
          flowcell_id,
          'all',
          'all',
          'all',
          'laneBarcode.html')
      dest_source_html_file = \
        os.path.join(
          output_path,
          'laneBarcode_{0}.html'.format(lane_id))
      copy_local_file(
        source_html_file,
        dest_source_html_file,
        force=True)
      upload_file_or_dir_to_box(
        box_config_file=BOX_CONFIG_FILE,
        file_path=dest_source_html_file,
        upload_dir=box_dir,
        box_username=BOX_USERNAME)
      stats_source_file = \
        os.path.join(
          tmp_bcl2fq_output,
          'Stats',
          'Stats.json')
      stats_dest_file = \
        os.path.join(
          output_path,
          'Stats_{0}.json'.format(lane_id))
      copy_local_file(
        stats_source_file,
        stats_dest_file,
        force=True)
      upload_file_or_dir_to_box(
        box_config_file=BOX_CONFIG_FILE,
        file_path=stats_dest_file,
        upload_dir=box_dir,
        box_username=BOX_USERNAME)
      samplesheet_dest_file = \
        os.path.join(
          output_path,
          'SampleSheet_{0}.csv'.format(lane_id))
      copy_local_file(
        samplesheet_file,
        samplesheet_dest_file,
        force=True)
      upload_file_or_dir_to_box(
        box_config_file=BOX_CONFIG_FILE,
        file_path=samplesheet_dest_file,
        upload_dir=box_dir,
        box_username=BOX_USERNAME)
    else:
      raise ValueError('No dag_run.conf entry found')
  except Exception as e:
    logging.error(e)
    message = \
      'Failed demultiplexing, error: {0}'.\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise


def prepare_merged_report_func(**context):
  try:
    dag_run = context.get('dag_run')
    ti = context.get('ti')
    output_path_xcom_task = \
      context['params'].get('output_path_xcom_task')
    output_path_xcom_key = \
      context['params'].get('output_path_xcom_key')
    script_path = \
      context['params'].get('script_path')
    template_path = \
      context['params'].get('template_path')
    code_dir = \
      context['params'].get('code_dir')
    if dag_run is not None and \
       dag_run.conf is not None:
      seqrun_id = dag_run.conf.get('seqrun_id')
      samplesheet_file_name = dag_run.conf.get('samplesheet_file_name')
      flowcell_id = dag_run.conf.get('flowcell_id')
      tag_id = dag_run.conf.get('tag_id')
      if seqrun_id is None or \
         flowcell_id is None or \
         tag_id is None or \
         samplesheet_file_name is None:
        raise ValueError('Missing samplesheet, flowcell id, tag_id or seqrun id')
      output_path = \
        ti.xcom_pull(
          key=output_path_xcom_key,
          task_ids=output_path_xcom_task)
      check_file_path(output_path)
      temp_dir = \
        get_temp_dir()
      samplesheet_list = \
        os.path.join(temp_dir, 'samplesheet_list')
      stats_list = \
        os.path.join(temp_dir, 'stats_list')
      output_report_file = \
        os.path.join(temp_dir, '{0}_{1}.html'.format(seqrun_id, tag_id))
      samplesheet_files = list()
      samplesheet_files = [
        os.path.join(output_path, f)
          for f in os.listdir(output_path)
            if f.startswith('SampleSheet_')]
      stats_files = [
        os.path.join(output_path, f)
          for f in os.listdir(output_path)
            if f.startswith('Stats_')]
      if not len(stats_files) > 0 or \
         len(stats_files) != len(samplesheet_files):
         raise ValueError(
                 'SampleSheet or Stats file not found: {0}'.\
                   format(output_path))
      with open(samplesheet_list, 'w') as fp:
        fp.write('\n'.join(samplesheet_files))
      with open(stats_list, 'w') as fp:
        fp.write('\n'.join(stats_files))
      args_list = [
        "python",
        script_path,
        "-i", seqrun_id,
        "-j", stats_list,
        "-s",  samplesheet_list,
        "-o", output_report_file,
        "-t", template_path]
      bind_dir_list = [
        temp_dir,
        output_path,
        code_dir]
      options = [
        "--no-home",
        "-C",
        "--env",
        "PYTHONPATH={0}".format(code_dir)]
      _ = \
        singularity_run(
          image_path=INTEROP_IMAGE,
          args_list=args_list,
          bind_dir_list=bind_dir_list,
          options=options)
      check_file_path(output_report_file)
      box_dir = \
        os.path.join(
          BOX_DIR_PREFIX,
          seqrun_id,
          tag_id)
      copy_local_file(
        output_report_file,
        os.path.join(output_path, '{0}_{1}.html'.format(seqrun_id, tag_id)),
        force=True)
      upload_file_or_dir_to_box(
        box_config_file=BOX_CONFIG_FILE,
        file_path=output_report_file,
        upload_dir=box_dir,
        box_username=BOX_USERNAME)
    else:
      raise ValueError('No dag_run.conf entry found')
  except Exception as e:
    logging.error(e)
    message = \
      'Failed merge report generation, error: {0}'.\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise