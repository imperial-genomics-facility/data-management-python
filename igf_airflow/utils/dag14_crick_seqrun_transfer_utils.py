import os, logging, subprocess, sys
from threading import Thread
import pandas as pd
from ftplib import FTP_TLS, error_temp
from airflow.models import Variable
from igf_data.utils.fileutils import get_temp_dir, read_json_data
from igf_data.utils.fileutils import remove_dir, copy_local_file
from igf_data.utils.fileutils import check_file_path
from igf_airflow.logging.upload_log_msg import send_log_to_channels

SLACK_CONF = Variable.get('slack_conf', default_var=None)
MS_TEAMS_CONF = Variable.get('ms_teams_conf', default_var=None)
HPC_SEQRUN_BASE_PATH = Variable.get('hpc_seqrun_path', default_var=None)
FTP_SEQRUN_SERVER = Variable.get('crick_ftp_seqrun_hostname', default_var=None)
FTP_CONFIG_FILE = Variable.get('crick_ftp_config_file_wells', default_var=None)

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