import os
import sys
import logging
import subprocess
from threading import Thread
import pandas as pd
from ftplib import FTP_TLS
from ftplib import error_temp
from airflow.models import Variable
from igf_data.utils.fileutils import get_temp_dir
from igf_data.utils.fileutils import read_json_data
from igf_data.utils.fileutils import remove_dir
from igf_data.utils.fileutils import copy_local_file
from igf_data.utils.fileutils import get_date_stamp
from igf_data.utils.fileutils import list_remote_file_or_dirs
from igf_data.utils.fileutils import copy_remote_file
from igf_data.utils.fileutils import check_file_path
from igf_airflow.logging.upload_log_msg import send_log_to_channels
from igf_data.utils.jupyter_nbconvert_wrapper import Notebook_runner
from igf_data.utils.box_upload import upload_file_or_dir_to_box


SLACK_CONF = Variable.get('slack_conf', default_var=None)
MS_TEAMS_CONF = Variable.get('ms_teams_conf', default_var=None)
HPC_SEQRUN_BASE_PATH = Variable.get('hpc_seqrun_path', default_var=None)
FTP_SEQRUN_SERVER = Variable.get('crick_ftp_seqrun_hostname', default_var=None)
FTP_CONFIG_FILE = Variable.get('crick_ftp_config_file_wells', default_var=None)
SEQRUN_SERVER = Variable.get('seqrun_server', default_var=None)
REMOTE_SEQRUN_BASE_PATH = Variable.get('seqrun_base_path', default_var=None)
SEQRUN_SERVER_USER = Variable.get('seqrun_server_user', default_var=None)
INTEROP_DUMPTEXT_EXE = Variable.get('interop_dumptext_exe', default_var=None)
INTEROP_NOTEBOOK_IMAGE_PATH = Variable.get('interop_notebook_image_path', default_var=None)
INTEROP_NOTEBOOK_TEMPLATE = Variable.get('interop_notebook_template', default_var=None)
SEQRUN_ML_NOTEBOOK_TEMPLATE = Variable.get('seqrun_ml_notebook_template', default_var=None)
SEQRUN_TRAINING_DATA_CSV = Variable.get('seqrun_training_data_csv', default_var=None)
BOX_DIR_PREFIX = Variable.get('box_dir_prefix_for_seqrun_report', default_var=None)
BOX_USERNAME = Variable.get('box_username', default_var=None)
BOX_CONFIG_FILE  = Variable.get('box_config_file', default_var=None)


def generate_interop_report_func(**context):
  try:
    ti = context.get('ti')
    dag_run = context.get('dag_run')
    seqrun_id = None
    runinfo_path = None
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('seqrun_id') is not None:
      seqrun_id = \
        dag_run.conf.get('seqrun_id')
      runinfo_path = \
        os.path.join(
          HPC_SEQRUN_BASE_PATH,
          seqrun_id,
          'RunInfo.xml')
    else:
      raise ValueError('seqrun id not found in dag_run.conf')
    interop_dump_xcom_task = \
      context['params'].get('interop_dump_xcom_task')
    timeout = \
      context['params'].get('timeout')
    kernel_name = \
      context['params'].get('kernel_name')
    interop_dump_path = \
      ti.xcom_pull(task_ids=interop_dump_xcom_task)
    check_file_path(interop_dump_path)
    temp_dir = \
      get_temp_dir(use_ephemeral_space=True)
    input_params = {
      'DATE_TAG': get_date_stamp(),
      'SEQRUN_IGF_ID': seqrun_id,
      'RUNINFO_XML_PATH': runinfo_path,
      'INTEROP_DUMP_PATH': interop_dump_path}
    container_bind_dir_list = [
      os.path.dirname(interop_dump_path),
      os.path.dirname(runinfo_path)]
    nb = \
      Notebook_runner(
        template_ipynb_path=INTEROP_NOTEBOOK_TEMPLATE,
        output_dir=temp_dir,
        input_param_map=input_params,
        container_paths=container_bind_dir_list,
        timeout=timeout,
        kernel=kernel_name,
        use_ephemeral_space=True,
        singularity_options=['--no-home','-C'],
        allow_errors=False,
        singularity_image_path=INTEROP_NOTEBOOK_IMAGE_PATH)
    output_notebook_path, _ = \
      nb.execute_notebook_in_singularity()
    box_dir = \
      os.path.join(BOX_DIR_PREFIX, seqrun_id)
    upload_file_or_dir_to_box(
      box_config_file=BOX_CONFIG_FILE,
      file_path=output_notebook_path,
      upload_dir=box_dir,
      box_username=BOX_USERNAME)
    input_params = {
      'SEQRUN_IGF_ID': seqrun_id,
      'RUNINFO_XML_PATH': runinfo_path,
      'INTEROP_DUMP_PATH': interop_dump_path,
      'SEQRUN_TRAINING_DATA': SEQRUN_TRAINING_DATA_CSV}
    container_bind_dir_list = [
      os.path.dirname(interop_dump_path),
      os.path.dirname(runinfo_path),
      os.path.dirname(SEQRUN_TRAINING_DATA_CSV) ]
    nb = \
      Notebook_runner(
        template_ipynb_path=SEQRUN_ML_NOTEBOOK_TEMPLATE,
        output_dir=temp_dir,
        input_param_map=input_params,
        container_paths=container_bind_dir_list,
        timeout=timeout,
        kernel=kernel_name,
        use_ephemeral_space=True,
        singularity_options=['--no-home','-C'],
        allow_errors=False,
        singularity_image_path=INTEROP_NOTEBOOK_IMAGE_PATH)
    output_notebook_path, _ = \
      nb.execute_notebook_in_singularity()
    upload_file_or_dir_to_box(
      box_config_file=BOX_CONFIG_FILE,
      file_path=output_notebook_path,
      upload_dir=box_dir,
      box_username=BOX_USERNAME)
  except Exception as e:
    logging.error(e)
    message = \
      'Failed to generate InterOp report, error: {0}'.\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise


def run_interop_dump_func(**context):
  try:
    seqrun_dir = None
    dag_run = context.get('dag_run')
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('seqrun_id') is not None:
      seqrun_id = \
        dag_run.conf.get('seqrun_id')
      seqrun_dir = \
        os.path.join(
          HPC_SEQRUN_BASE_PATH, seqrun_id)
    else:
      raise ValueError('seqrun id not found in dag_run.conf')
    temp_dir = \
      get_temp_dir(use_ephemeral_space=True)
    dump_file = \
      os.path.join(
        temp_dir,
        '{0}_interop_dump.csv'.format(seqrun_id))
    check_file_path(INTEROP_DUMPTEXT_EXE)
    cmd = [
      INTEROP_DUMPTEXT_EXE, seqrun_dir, '>', dump_file]
    cmd = ' '.join(cmd)
    subprocess.\
      check_call(cmd, shell=True)
    return dump_file
  except Exception as e:
    logging.error(e)
    message = \
      'Failed to generate InterOp dump, error: {0}'.\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise

def copy_run_file_to_remote_func(**context):
  try:
    ti = context.get('ti')
    xcom_task = \
      context['params'].get('xcom_task')
    xcom_key = \
      context['params'].get('xcom_key')
    if xcom_key is not None and \
       xcom_key == 'bcl_files':
      lane_id = \
        context['params'].get('lane_id')
      if lane_id is None:
        raise ValueError('No lane id found for bcl file copy')
      xcom_data = \
        ti.xcom_pull(
          task_ids=xcom_task, key=xcom_key)
      if xcom_data is None or \
         (xcom_data is not None and not isinstance(xcom_data, dict)):
        raise ValueError('xcom data is not correctly formatted')
      if xcom_data is not None and \
         isinstance(xcom_data, dict) and \
         lane_id not in xcom_data.keys():
        raise ValueError(
                'No xcom entry for lane {0} found'.\
                  format(lane_id))
      xcom_data_for_lane = \
        xcom_data.get(lane_id)
      if not isinstance(xcom_data_for_lane, dict) or \
         'local_bcl_path' not in xcom_data_for_lane.keys() or \
         xcom_data_for_lane.get('local_bcl_path') is None or \
         'remote_bcl_path' not in xcom_data_for_lane.keys() or \
         xcom_data_for_lane.get('remote_bcl_path') is None:
        raise ValueError('Local or remote bcl file path not found')
      local_bcl_path = \
        xcom_data_for_lane.get('local_bcl_path')
      remote_bcl_path = \
        xcom_data_for_lane.get('remote_bcl_path')
      copy_remote_file(
        source_path=local_bcl_path,
        destination_path=remote_bcl_path,
        destination_address='{0}@{1}'.format(SEQRUN_SERVER_USER, SEQRUN_SERVER))
    elif xcom_key is not None and \
       xcom_key == 'additional_files':
      xcom_data = \
        ti.xcom_pull(
          task_ids=xcom_task, key=xcom_key)
      if xcom_data is None or \
         (xcom_data is not None and not isinstance(xcom_data, dict)):
        raise ValueError('xcom data is not correctly formatted')
      if xcom_data is not None and \
         isinstance(xcom_data, dict) and \
         len(xcom_data.keys())==0:
        raise ValueError('No xcom entry for additional files found')
      for f in xcom_data.keys():
        local_file_path = \
          xcom_data[f].get('local_file_path')
        remote_file_path = \
          xcom_data[f].get('remote_file_path')
        if local_file_path is None or \
           remote_file_path is None:
          raise ValueError(
                  'Local or remote file not found for {0}'.format(f))
        copy_remote_file(
          source_path=local_file_path,
          destination_path=remote_file_path,
          destination_address='{0}@{1}'.format(SEQRUN_SERVER_USER, SEQRUN_SERVER))
  except Exception as e:
    logging.error(e)
    message = \
      'Failed to copy bcl file to remote, error: {0}'.\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise

def check_and_divide_run_for_remote_copy_func(**context):
  try:
    ti = context.get('ti')
    seqrun_dir = None
    remote_seqrun_dir = None
    dag_run = context.get('dag_run')
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('seqrun_id') is not None:
      seqrun_id = \
        dag_run.conf.get('seqrun_id')
      seqrun_dir = \
        os.path.join(
          HPC_SEQRUN_BASE_PATH, seqrun_id)
      remote_seqrun_dir = \
        os.path.join(
          REMOTE_SEQRUN_BASE_PATH, seqrun_id)
    else:
      raise ValueError('seqrun id not found in dag_run.conf')
    # check for remote run dir and exit
    all_seqrun_ids = \
      list_remote_file_or_dirs(
        remote_server=SEQRUN_SERVER,
        remote_path=REMOTE_SEQRUN_BASE_PATH,
        user_name=SEQRUN_SERVER_USER,
        only_dirs=True)
    if seqrun_id in all_seqrun_ids:
      raise IOError(
              'Seqrun {0} already present on the rempote server {1}, path {2}, remove it before copy'.\
                format(seqrun_id, SEQRUN_SERVER, REMOTE_SEQRUN_BASE_PATH))
    xcom_bcl_dict = dict()
    for i in range(1, 9):
      local_bcl_path = \
        os.path.join(
          seqrun_dir,
          'Data',
          'Intensities',
          'BaseCalls',
          'L00{0}'.format(i))
      remote_bcl_path = \
        os.path.join(
          remote_seqrun_dir,
          'Data',
          'Intensities',
          'BaseCalls')
      check_file_path(local_bcl_path)
      xcom_bcl_dict.\
        update({
          i: {
            'local_bcl_path': local_bcl_path,
            'remote_bcl_path': remote_bcl_path}})
    additional_files = [
      'RunInfo.xml',
      'runParameters.xml',
      'InterOp',
      'Logs',
      'Recipe',
      'RTAComplete.txt',
      'Data/Intensities/s.locs']
    xcom_additional_files_dict = dict()
    for f in additional_files:
      local_file_path = \
        os.path.join(
          seqrun_dir, f)
      remote_file_path = \
        os.path.join(
          remote_seqrun_dir, f)
      check_file_path(local_file_path)
      xcom_additional_files_dict.\
        update({
          f: {
            'local_file_path': local_file_path,
            'remote_file_path': remote_file_path}})
    ti.xcom_push(
      key='bcl_files',
      value=xcom_bcl_dict)
    ti.xcom_push(
      key='additional_files',
      value=xcom_additional_files_dict)
    task_list = [
      'copy_bcl_to_remote_for_lane{0}'.format(i)
        for i in range(1,9)]
    task_list.\
      append('copy_additional_files')
    return task_list
  except Exception as e:
    logging.error(e)
    message = \
      'Failed remote copy decide branch, error: {0}'.\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise

def validate_md5_chunk_func(**context):
  try:
    seqrun_dir = None
    dag_run = context.get('dag_run')
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('seqrun_id') is not None:
      seqrun_id = \
        dag_run.conf.get('seqrun_id')
      seqrun_dir = \
        os.path.join(
          HPC_SEQRUN_BASE_PATH, seqrun_id)
    else:
      raise ValueError('seqrun id not found in dag_run.conf')
    chunk_id = \
      context['params'].get('chunk_id')
    xcom_task = \
      context['params'].get('xcom_task')
    xcom_key = \
      context['params'].get('xcom_key')
    ti = \
      context.get('ti')
    md5_chunk_dict = \
      ti.xcom_pull(
        task_ids=xcom_task,
        key=xcom_key)
    if int(chunk_id) not in md5_chunk_dict.keys():
      raise ValueError(
              'Missing chunk id {0} in the md5 chunks {1}'.\
                format(chunk_id, md5_chunk_dict))
    md5_file_chunk = \
      md5_chunk_dict.\
        get(int(chunk_id))
    check_file_path(md5_file_chunk)
    cmd = \
      "cd {0}; md5sum -c {1}".\
        format(seqrun_dir, md5_file_chunk)
    temp_dir = \
      get_temp_dir(use_ephemeral_space=True)
    error_file = \
      os.path.join(
        temp_dir,
        '{0}.err'.format(os.path.basename(md5_file_chunk)))
    try:
      with open(error_file, 'w') as fp:
        _ = \
          subprocess.\
            check_call(
              cmd,
              stderr=fp,
              shell=True)
    except Exception as e:
      raise ValueError(
              'Error in md5: {0}, log: {1}'.format(e, error_file))
  except Exception as e:
    logging.error(e)
    message = \
      'Failed md5 validation, error: {0}'.\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise


def _split_md5_file(md5_file, trim_path, temp_dir, split_count=50):
  try:
    df = \
      pd.read_csv(
        md5_file,
        sep='\s+',
        header=None,
        names=['md5', 'file_path'])
    start = 0
    counter = 0
    max_lines = len(df.index)
    lines_per_split = \
      int(max_lines / split_count) + 1
    chunk_file_dict = dict()
    df['file_path'] = \
      df['file_path'].\
        str.\
          replace(trim_path, '')
    df['file_path'] = \
      df['file_path'].\
        str.\
          lstrip('/')
    while start < max_lines:
      chunk_file = \
        os.path.join(
          temp_dir,
          '{0}_{1}'.format(
            os.path.basename(md5_file),
            counter))
      finish = start + lines_per_split
      if finish > max_lines:
        finish = max_lines
      df.iloc[start:finish].\
        to_csv(
          chunk_file,
          sep="\t",
          index=False,
          header=False)
      chunk_file_dict.\
        update({counter: chunk_file})
      start = finish
      counter += 1
    return chunk_file_dict
  except Exception as e:
    raise ValueError(
            'Failed to split md5 file {0}, error: {1}'.\
              format(md5_file, e))


def find_and_split_md5_func(**context):
  try:
    ti = context.get('ti')
    split_count = \
      context['params'].get('split_count')
    dag_run = context.get('dag_run')
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('seqrun_id') is not None:
      seqrun_id = \
        dag_run.conf.get('seqrun_id')
      seqrun_dir = \
        os.path.join(
          HPC_SEQRUN_BASE_PATH, seqrun_id)
      md5_file = \
        os.path.join(
          seqrun_dir,
          '{0}.md5'.format(seqrun_id))
      check_file_path(md5_file)
      temp_dir = \
        get_temp_dir(use_ephemeral_space=True)
      trim_path = \
        os.path.join(
          'camp',
          'stp',
          'sequencing',
          'inputs',
          'instruments',
          'sequencers',
          seqrun_id)
      md5_file_chunks = \
        _split_md5_file(
          md5_file=md5_file,
          trim_path=trim_path,
          temp_dir=temp_dir,
          split_count=split_count)
      ti.xcom_push(key='md5_file_chunk', value=md5_file_chunks)
      return ['md5_validate_chunk_{0}'.format(i) for i in range(0, split_count)]
    else:
      raise ValueError('seqrun id not found in dag_run.conf')
  except Exception as e:
    logging.error(e)
    message = \
      'Failed md5 split, error: {0}'.\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise


def extract_tar_file_func(**context):
  try:
    dag_run = context.get('dag_run')
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('seqrun_id') is not None:
      seqrun_id = \
        dag_run.conf.get('seqrun_id')
      seqrun_tar_file = \
        os.path.join(
          HPC_SEQRUN_BASE_PATH,
          '{0}.tar.gz'.format(seqrun_id))
      output_seqrun_dir = \
        _extract_seqrun_tar(
          tar_file=seqrun_tar_file,
          seqrun_id=seqrun_id,
          seqrun_base_path=HPC_SEQRUN_BASE_PATH)
      message = \
        'Extracted run {0} to {1}'.\
          format(seqrun_id, output_seqrun_dir)
      send_log_to_channels(
        slack_conf=SLACK_CONF,
        ms_teams_conf=MS_TEAMS_CONF,
        task_id=context['task'].task_id,
        dag_id=context['task'].dag_id,
        comment=message,
        reaction='pass')
    else:
      raise ValueError('seqrun id not found in dag_run.conf')
  except Exception as e:
    logging.error(e)
    message = \
      'Failed tar file extraction, error: {0}'.\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise


def _extract_seqrun_tar(tar_file, seqrun_id, seqrun_base_path):
  try:
    check_file_path(tar_file)
    output_seqrun_dir = \
      os.path.join(
        seqrun_base_path,
        seqrun_id)
    if os.path.exists(output_seqrun_dir):
      raise ValueError(
              'Dir {0} already present, remove it before re-run'.\
                format(output_seqrun_dir))
    temp_dir = \
      get_temp_dir(use_ephemeral_space=False)
    if os.path.exists(temp_dir):
      _change_temp_dir_permissions(temp_dir)
      remove_dir(temp_dir)
    os.makedirs(temp_dir, exist_ok=False)
    base_run_path = \
      os.path.join(
        temp_dir,
        'camp',
        'stp',
        'sequencing',
        'inputs',
        'instruments',
        'sequencers',
        seqrun_id)
    untar_command = \
      "tar --no-same-owner --no-same-permissions --owner=igf -C {0} -xzf {1}".\
        format(temp_dir, tar_file)
    subprocess.\
      check_call(
        untar_command, shell=True)
    _change_temp_dir_permissions(temp_dir)
    if not os.path.exists(base_run_path):
      raise IOError(
              'Path {0} not found. List of files under temp dir: {1}'.\
                format(base_run_path, os.listdir(temp_dir)))
    logging.warn('Run extracted, seqrun_id: {0}, path: {1}'.\
      format(seqrun_id, base_run_path))
    copy_local_file(
      base_run_path,
      output_seqrun_dir)
    logging.warn('Run copied, seqrun_id: {0}, path: {1}'.\
      format(seqrun_id, output_seqrun_dir))
    return output_seqrun_dir
  except Exception as e:
    raise ValueError(e)


def _change_temp_dir_permissions(temp_dir):
  try:
    dir_permission_cmd1 = \
      "find {0} -type d -exec chmod 700 {{}} \;".format(temp_dir)
    subprocess.\
      check_call(
        dir_permission_cmd1,
        shell=True)
    chmod_cmd1 = \
      "chmod -R u+r {0}".format(temp_dir)
    subprocess.\
      check_call(
        chmod_cmd1,
        shell=True)
    chmod_cmd2 = \
      "chmod -R u+w {0}".format(temp_dir)
    subprocess.\
      check_call(
        chmod_cmd2,
        shell=True)
  except Exception as e:
    raise ValueError(e)


def check_and_transfer_run_func(**context):
  try:
    dag_run = context.get('dag_run')
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('seqrun_id') is not None:
      seqrun_id = \
        dag_run.conf.get('seqrun_id')
      transfer_seqrun_tar_from_crick_ftp(
        ftp_host=FTP_SEQRUN_SERVER,
        ftp_conf_file=FTP_CONFIG_FILE,
        seqrun_id=seqrun_id,
        seqrun_base_dir=HPC_SEQRUN_BASE_PATH)
      message = \
        'Finish downloading seqrun {0} from FTP'.\
          format(seqrun_id)
      send_log_to_channels(
        slack_conf=SLACK_CONF,
        ms_teams_conf=MS_TEAMS_CONF,
        task_id=context['task'].task_id,
        dag_id=context['task'].dag_id,
        comment=message,
        reaction='pass')
    else:
      raise ValueError('seqrun id not found in dag_run.conf')
  except Exception as e:
    logging.error(e)
    message = \
      'Failed FTP file download, error: {0}'.\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise


def warn_exisitng_seqrun_paths(seqrun_id, seqrun_base_dir, clean_up_tar=False):
  try:
      seqrun_path = \
        os.path.join(
          seqrun_base_dir,
          seqrun_id)
      seqrun_tar_file = \
        os.path.join(
          seqrun_base_dir,
          '{0}.tar.gz'.format(seqrun_id))
      if os.path.exists(seqrun_path):
        raise ValueError(
                'Directory for seqrun {0} already present'.\
                  format(seqrun_id))
      if os.path.exists(seqrun_tar_file):
        if not clean_up_tar:
          raise ValueError(
                  'tar file for seqrun {0} already present'.\
                    format(seqrun_id))
        else:
          logging.warn('removing tar {0}'.format(seqrun_tar_file))
          os.remove(seqrun_tar_file)
  except Exception as e:
    raise ValueError('Error, tar or dir exists: {0}: {1}'.\
            format(seqrun_id, e))


def background_handle(ftps, remote_path, local_path):
  try:
    total_size = ftps.size(remote_path)
    file_to_write = open(local_path, 'wb')
    sock = ftps.transfercmd('RETR ' + remote_path)
    while True:
      block = sock.recv(1024*1024)
      if not block:
        break
      file_to_write.write(block)
      sizeWritten = file_to_write.tell()
      bar_len = 100
      percent = round(100 * sizeWritten / total_size, 1)
      filled_len = int(percent)
      bar = '=' * filled_len + '-' * (bar_len - filled_len)
      sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percent, '%', ''))
      sys.stdout.flush()
    sock.close()
    logging.warn(
      'Closing file {0}'.format(local_path))
    file_to_write.close()
  except Exception as e:
    raise ValueError(e)


def transfer_seqrun_tar_from_crick_ftp(
      ftp_host, ftp_conf_file, seqrun_id, seqrun_base_dir, ftp_timeout=600):
  try:
    if seqrun_id is None or \
       seqrun_id == "":
      raise ValueError('Missing valid seqrun id')
    logging.warn('Seqrun: {0}'.format(seqrun_id))
    warn_exisitng_seqrun_paths(
      seqrun_id=seqrun_id,
      seqrun_base_dir=seqrun_base_dir,
      clean_up_tar=False)
    ftp_conf = \
      read_json_data(ftp_conf_file)[0]
    ftps = FTP_TLS(timeout=ftp_timeout)
    ftps.connect(ftp_host)
    ftps.login(
      ftp_conf.get('username'),
      ftp_conf.get('password'))
    ftp_files = \
      ftps.nlst('/users/{0}/runs'.format(ftp_conf.get('username')))
    seqrun_tar_file = \
      '{0}.tar.gz'.format(seqrun_id)
    seqrun_tar_file_path = \
      os.path.join(seqrun_base_dir, seqrun_tar_file)
    temp_dir = \
      get_temp_dir(use_ephemeral_space=False)
    temp_seqrun_tar_file = \
      os.path.join(
        temp_dir, seqrun_tar_file)
    if seqrun_tar_file not in ftp_files:
      raise ValueError(
              'Run {0} not found on FTP'.\
                format(seqrun_tar_file))
    remote_file_path = \
      '/users/{0}/runs/{1}'.format(
        ftp_conf.get('username'),
        seqrun_tar_file)
    ftp_file_size = \
      int(ftps.size(remote_file_path))
    t = \
      Thread(
        target=background_handle,
          args=(
            ftps,
            remote_file_path,
            temp_seqrun_tar_file))
    t.start()
    while t.is_alive():
      t.join(400)
      try:
        ftps.voidcmd('NOOP')
      except error_temp as e:
        logging.warn(e)
    logging.warn('downloaded tar {0}'.format(temp_seqrun_tar_file))
    file_size = \
      os.stat(temp_seqrun_tar_file).st_size
    if int(file_size) != int(ftp_file_size):
      raise ValueError(
              'FTP file size {0} and local file size {1} are not same'.\
                format(ftp_file_size, file_size))
    else:
      copy_local_file(
        temp_seqrun_tar_file,
        seqrun_tar_file_path)
      logging.warn('Copied tar to {0}'.format(seqrun_tar_file_path))
  except Exception as e:
    raise ValueError(
            'Failed to download FTP file for run {0}: error: {1}'.\
              format(seqrun_id, e))