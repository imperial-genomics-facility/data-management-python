import os,logging,subprocess
from airflow.models import Variable
from igf_data.illumina.runparameters_xml import RunParameter_xml
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.utils.box_upload import upload_file_or_dir_to_box
from igf_airflow.seqrun.ongoing_seqrun_processing import fetch_ongoing_seqruns,compare_existing_seqrun_files
from igf_airflow.seqrun.ongoing_seqrun_processing import check_for_sequencing_progress
from igf_airflow.logging.upload_log_msg import send_log_to_channels,log_success,log_failure,log_sleep
from igf_data.utils.fileutils import get_temp_dir,copy_remote_file,check_file_path,read_json_data
from igf_data.utils.samplesheet_utils import samplesheet_validation_and_metadata_checking

def get_ongoing_seqrun_list(**context):
  """
  A function for fetching ongoing sequencing run ids
  """
  try:
    ti = context.get('ti')
    seqrun_server = Variable.get('seqrun_server')
    seqrun_base_path = Variable.get('seqrun_base_path')
    seqrun_server_user = Variable.get('seqrun_server_user')
    database_config_file = Variable.get('database_config_file')
    ongoing_seqruns = \
      fetch_ongoing_seqruns(
        seqrun_server=seqrun_server,
        seqrun_base_path=seqrun_base_path,
        user_name=seqrun_server_user,
        database_config_file=database_config_file)
    ti.xcom_push(key='ongoing_seqruns',value=ongoing_seqruns)
    branch_list = ['generate_seqrun_file_list_{0}'.format(i[0]) 
                     for i in enumerate(ongoing_seqruns)]
    if len(branch_list) == 0:
      branch_list = ['no_ongoing_seqrun']
    else:
      send_log_to_channels(
        slack_conf=Variable.get('slack_conf'),
        ms_teams_conf=Variable.get('ms_teams_conf'),
        task_id=context['task'].task_id,
        dag_id=context['task'].dag_id,
        comment='Ongoing seqruns found: {0}'.format(ongoing_seqruns),
        reaction='pass')
    return branch_list
  except Exception as e:
    logging.error(e)
    send_log_to_channels(
      slack_conf=Variable.get('slack_conf'),
      ms_teams_conf=Variable.get('ms_teams_conf'),
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def copy_seqrun_manifest_file(**context):
  """
  A function for copying filesize manifest for ongoing sequencing runs to hpc 
  """
  try:
    remote_file_path = context['params'].get('file_path')
    seqrun_server = Variable.get('seqrun_server')
    seqrun_server_user = Variable.get('seqrun_server_user')
    xcom_pull_task_ids = context['params'].get('xcom_pull_task_ids')
    ti = context.get('ti')
    remote_file_path = ti.xcom_pull(task_ids=xcom_pull_task_ids)
    if remote_file_path is not None and \
       not isinstance(remote_file_path,str):
      remote_file_path = remote_file_path.decode().strip('\n')
    tmp_work_dir = get_temp_dir(use_ephemeral_space=True)
    local_file_path = \
      os.path.join(
        tmp_work_dir,
        os.path.basename(remote_file_path))
    remote_address = \
      '{0}@{1}'.format(seqrun_server_user,seqrun_server)
    copy_remote_file(
      remote_file_path,
      local_file_path,
      source_address=remote_address)
    return local_file_path
  except Exception as e:
    logging.error(e)
    send_log_to_channels(
      slack_conf=Variable.get('slack_conf'),
      ms_teams_conf=Variable.get('ms_teams_conf'),
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def reset_manifest_file(**context):
  """
  A function for checking existing files and resetting the manifest json with new files
  """
  try:
    xcom_pull_task_ids = context['params'].get('xcom_pull_task_ids')
    local_seqrun_path = context['params'].get('local_seqrun_path')
    seqrun_id_pull_key = context['params'].get('seqrun_id_pull_key')
    seqrun_id_pull_task_ids = context['params'].get('seqrun_id_pull_task_ids')
    run_index_number = context['params'].get('run_index_number')
    ti = context.get('ti')
    json_path = ti.xcom_pull(task_ids=xcom_pull_task_ids)
    if json_path is not None and \
       not isinstance(json_path,str):
      json_path = json_path.decode().strip('\n')
    seqrun_id = \
      ti.xcom_pull(key=seqrun_id_pull_key,task_ids=seqrun_id_pull_task_ids)[run_index_number]
    compare_existing_seqrun_files(
      json_path=json_path,
      seqrun_id=seqrun_id,
      seqrun_base_path=local_seqrun_path)
  except Exception as e:
    logging.error(e)
    send_log_to_channels(
      slack_conf=Variable.get('slack_conf'),
      ms_teams_conf=Variable.get('ms_teams_conf'),
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def get_seqrun_chunks(**context):
  """
  A function for setting file chunk size for seqrun files copy
  """
  try:
    ti = context.get('ti')
    worker_size = context['params'].get('worker_size')
    child_task_prefix = context['params'].get('child_task_prefix')
    seqrun_chunk_size_key = context['params'].get('seqrun_chunk_size_key')
    xcom_pull_task_ids = context['params'].get('xcom_pull_task_ids')
    file_path = ti.xcom_pull(task_ids=xcom_pull_task_ids)
    if file_path is not None and \
       not isinstance(file_path,str):
      file_path = file_path.decode().strip('\n')
    check_file_path(file_path)
    file_data = read_json_data(file_path)
    chunk_size = None
    if worker_size is None or \
       worker_size == 0:
      raise ValueError(
              'Incorrect worker size: {0}'.\
                format(worker_size))
    if len(file_data) == 0:
      worker_branchs = \
        '{0}_{1}'.format(child_task_prefix,'no_work')
    else:
      if len(file_data) < int(5 * worker_size):
        worker_size = 1                                                           # setting worker size to 1 for low input
      if len(file_data) % worker_size == 0:
        chunk_size = int(len(file_data) / worker_size)
      else:
        chunk_size = int(len(file_data) / worker_size)+1
      ti.xcom_push(key=seqrun_chunk_size_key,value=chunk_size)
      worker_branchs = \
        ['{0}_{1}'.format(child_task_prefix,i) 
           for i in range(worker_size)]
    return worker_branchs
  except Exception as e:
    logging.error(e)
    send_log_to_channels(
      slack_conf=Variable.get('slack_conf'),
      ms_teams_conf=Variable.get('ms_teams_conf'),
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def copy_seqrun_chunk(**context):
  """
  A function for copying seqrun chunks
  """
  try:
    ti = context.get('ti')
    file_path_task_ids = context['params'].get('file_path_task_ids')
    seqrun_chunk_size_key = context['params'].get('seqrun_chunk_size_key')
    seqrun_chunk_size_task_ids = context['params'].get('seqrun_chunk_size_task_ids')
    chunk_index_number = context['params'].get('chunk_index_number')
    run_index_number = context['params'].get('run_index_number')
    local_seqrun_path = context['params'].get('local_seqrun_path')
    seqrun_id_pull_key = context['params'].get('seqrun_id_pull_key')
    seqrun_id_pull_task_ids = context['params'].get('seqrun_id_pull_task_ids')
    seqrun_server = Variable.get('seqrun_server')
    seqrun_server_user = Variable.get('seqrun_server_user')
    seqrun_base_path = Variable.get('seqrun_base_path')
    seqrun_id = \
      ti.xcom_pull(key=seqrun_id_pull_key,task_ids=seqrun_id_pull_task_ids)[run_index_number]
    file_path = \
      ti.xcom_pull(task_ids=file_path_task_ids)
    chunk_size = \
      ti.xcom_pull(key=seqrun_chunk_size_key,task_ids=seqrun_chunk_size_task_ids)
    check_file_path(file_path)
    file_data = read_json_data(file_path)
    start_index = chunk_index_number*chunk_size
    finish_index = ((chunk_index_number+1)*chunk_size) - 1
    if finish_index > len(file_data) - 1:
      finish_index = len(file_data) - 1
    local_seqrun_path = \
      os.path.join(
        local_seqrun_path,
        seqrun_id)
    remote_seqrun_path = \
      os.path.join(
        seqrun_base_path,
        seqrun_id)
    remote_address = \
      '{0}@{1}'.format(
        seqrun_server_user,
        seqrun_server)
    for entry in file_data[start_index:finish_index]:
      file_path = entry.get('file_path')
      file_size = entry.get('file_size')
      remote_path = \
        os.path.join(
          remote_seqrun_path,
          file_path)
      local_path = \
        os.path.join(
          local_seqrun_path,
          file_path)
      if os.path.exists(local_path) and \
         os.path.getsize(local_path) == file_size:
        pass
      else:
        copy_remote_file(
          remote_path,
          local_path,
          source_address=remote_address,
          check_file=False)
  except Exception as e:
    logging.error(e)
    send_log_to_channels(
      slack_conf=Variable.get('slack_conf'),
      ms_teams_conf=Variable.get('ms_teams_conf'),
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def run_interop_dump(**context):
  """
  A function for generating InterOp dump for seqrun
  """
  try:
    ti = context.get('ti')
    local_seqrun_path = Variable.get('hpc_seqrun_path')
    seqrun_id_pull_key = context['params'].get('seqrun_id_pull_key')
    seqrun_id_pull_task_ids = context['params'].get('seqrun_id_pull_task_ids')
    run_index_number = context['params'].get('run_index_number')
    interop_dumptext_exe = Variable.get('interop_dumptext_exe')
    temp_dir = get_temp_dir(use_ephemeral_space=True)
    seqrun_id = \
      ti.xcom_pull(key=seqrun_id_pull_key,task_ids=seqrun_id_pull_task_ids)[run_index_number]
    seqrun_path = \
      os.path.join(
        local_seqrun_path,
        seqrun_id)
    dump_file = \
      os.path.join(
        temp_dir,
        '{0}_interop_dump.csv'.format(seqrun_id))
    check_file_path(interop_dumptext_exe)
    cmd = \
      [interop_dumptext_exe,seqrun_path,'>',dump_file]
    cmd = ' '.join(cmd)
    subprocess.\
      check_call(cmd,shell=True)
    return dump_file
  except Exception as e:
    logging.error(e)
    send_log_to_channels(
      slack_conf=Variable.get('slack_conf'),
      ms_teams_conf=Variable.get('ms_teams_conf'),
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def check_progress_for_run_func(**context):
  try:
    ti = context.get('ti')
    local_seqrun_path = Variable.get('hpc_seqrun_path')
    seqrun_id_pull_key = context['params'].get('seqrun_id_pull_key')
    seqrun_id_pull_task_ids = context['params'].get('seqrun_id_pull_task_ids')
    run_index_number = context['params'].get('run_index_number')
    interop_dump_pull_task = context['params'].get('interop_dump_pull_task')
    no_job_prefix = context['params'].get('no_job_prefix')
    next_job_prefix = context['params'].get('next_job_prefix')
    job_list = \
      ['{0}_{1}'.format(no_job_prefix,run_index_number)]
    seqrun_id = \
      ti.xcom_pull(key=seqrun_id_pull_key,task_ids=seqrun_id_pull_task_ids)[run_index_number]
    runinfo_path = \
      os.path.join(
        local_seqrun_path,
        seqrun_id,
        'RunInfo.xml')
    interop_dump_path = \
      ti.xcom_pull(task_ids=interop_dump_pull_task)
    if interop_dump_path is not None and \
       not isinstance(interop_dump_path,str):
      interop_dump_path = \
        interop_dump_path.decode().strip('\n')
    current_cycle,index_cycle_status,read_format = \
      check_for_sequencing_progress(
        interop_dump=interop_dump_path,
        runinfo_file=runinfo_path)
    comment = \
      'seqrun: {0}, current cycle: {1}, index cycle: {2}, read format: {3}'.\
        format(seqrun_id,current_cycle,index_cycle_status,read_format)
    send_log_to_channels(
      slack_conf=Variable.get('slack_conf'),
      ms_teams_conf=Variable.get('ms_teams_conf'),
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=comment,
      reaction='pass')
    if index_cycle_status is 'complete':
      job_list = \
        ['{0}_{1}'.format(next_job_prefix,run_index_number)]
    return job_list
  except Exception as e:
    logging.error(e)
    send_log_to_channels(
      slack_conf=Variable.get('slack_conf'),
      ms_teams_conf=Variable.get('ms_teams_conf'),
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def samplesheet_validation_and_branch_func(**context):
  try:
    ti = context.get('ti')
    local_seqrun_path = Variable.get('hpc_seqrun_path')
    box_dir_prefix = Variable.get('box_dir_prefix_for_seqrun_report')
    box_username = Variable.get('box_username')
    box_config_file  = Variable.get('box_config_file')
    run_index_number = context['params'].get('run_index_number')
    seqrun_id_pull_key  = context['params'].get('seqrun_id_pull_key')
    seqrun_id_pull_task_ids  = context['params'].get('seqrun_id_pull_task_ids')
    samplesheet_file_name  = context['params'].get('samplesheet_file_name')
    runParameters_xml_file_name  = context['params'].get('runParameters_xml_file_name')
    no_job_prefix  = context['params'].get('no_job_prefix')
    next_job_prefix  = context['params'].get('next_job_prefix')
    next_job_range  = context['params'].get('next_job_range')
    samplesheet_validation_json  = context['params'].get('samplesheet_validation_json')
    job_list = \
      ['{0}_{1}'.format(no_job_prefix,run_index_number)]
    seqrun_id = \
      ti.xcom_pull(key=seqrun_id_pull_key,task_ids=seqrun_id_pull_task_ids)[run_index_number]
    samplesheet_path = \
      os.path.join(
        local_seqrun_path,
        seqrun_id,
        samplesheet_file_name)
    runParameters_path = \
      os.path.join(
        local_seqrun_path,
        seqrun_id,
        runParameters_xml_file_name)
    if os.path.exists(runParameters_path):
      runparameters_data = \
        RunParameter_xml(xml_file=runParameters_path)
      flowcell_type = \
        runparameters_data.\
          get_hiseq_flowcell()
      if flowcell_type is not None and \
         flowcell_type.startswith('HiSeq'):
        samplesheet = \
          SampleSheet(samplesheet_path)
        lanes = \
          samplesheet.\
            get_lane_count()
        for lane in lanes:
          if lane in next_job_range:
            next_job = \
              '{0}_{1}_{2}'.format(
                next_job_prefix,
                run_index_number,
                lane)
            job_list.\
              append(next_job)
        log_dir = get_temp_dir()
        validation_output_list = \
          samplesheet_validation_and_metadata_checking(
            samplesheet_file=samplesheet_path,
            schema_json_file=samplesheet_validation_json,
            log_dir=log_dir,
            seqrun_id=seqrun_id,
            db_config_file=box_config_file)
        if len(validation_output_list) > 0:
          _ = \
            [check_file_path(i)
              for i in validation_output_list]
          box_dir = \
            os.path.join(box_dir_prefix,seqrun_id)
          for file_path in validation_output_list:
            upload_file_or_dir_to_box(
              box_config_file=box_config_file,
              file_path=file_path,
              upload_dir=box_dir,
              box_username=box_username)
          message = \
            'Samplesheet validation failed for seqrun: `{0}`, box dir: `{1}`'.\
              format(seqrun_id,box_dir)
          send_log_to_channels(
            slack_conf=Variable.get('slack_conf'),
            ms_teams_conf=Variable.get('ms_teams_conf'),
            task_id=context['task'].task_id,
            dag_id=context['task'].dag_id,
            comment=message,
            reaction='pass')
    return job_list
  except Exception as e:
    logging.error(e)
    send_log_to_channels(
      slack_conf=Variable.get('slack_conf'),
      ms_teams_conf=Variable.get('ms_teams_conf'),
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise