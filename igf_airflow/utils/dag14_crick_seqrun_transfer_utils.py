import os, logging, subprocess, sys, threading
from shutil import move
from ftplib import FTP_TLS, error_temp
from airflow.models import Variable
from igf_data.utils.fileutils import get_temp_dir, read_json_data
from igf_data.utils.fileutils import remove_dir, copy_local_file
from igf_data.utils.fileutils import check_file_path
from igf_airflow.logging.upload_log_msg import send_log_to_channels

def extract_tar_file_func(**context):
  try:
    SLACK_CONF = Variable.get('slack_conf', default_var=None)
    MS_TEAMS_CONF = Variable.get('ms_teams_conf', default_var=None)
    HPC_SEQRUN_BASE_PATH = Variable.get('hpc_seqrun_path', default_var=None)
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
      os.path.join(
        seqrun_base_path,
        'temp_{0}'.format(seqrun_id))
    if os.path.exists(temp_dir):
      _change_temp_dir_permissions(temp_dir)
      remove_dir(temp_dir)
    os.makedirs(temp_dir, exist_ok=False)
    untar_command = \
      "tar --no-same-owner --no-same-permissions --owner=igf -C {0} -xzf {1}".format(temp_dir, tar_file)
    subprocess.\
      check_call(
        untar_command, shell=True)
    _change_temp_dir_permissions(temp_dir)
    source_path = \
      os.path.join(
        temp_dir,
        'camp',
        'stp',
        'sequencing',
        'inputs',
        'instruments',
        'sequencers',
        seqrun_id)
    check_file_path(source_path)
    move(
      src=source_path,
      dst=output_seqrun_dir)
    return output_seqrun_dir
  except Exception as e:
    raise ValueError(e)


def _change_temp_dir_permissions(temp_dir):
  try:
    dir_permission_cmd1 = \
      "find {0} -type d -exec chmod 700 \{\} \;".format(temp_dir)
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
    SLACK_CONF = Variable.get('slack_conf', default_var=None)
    MS_TEAMS_CONF = Variable.get('ms_teams_conf', default_var=None)
    FTP_SEQRUN_SERVER = Variable.get('crick_ftp_seqrun_hostname', default_var=None)
    FTP_CONFIG_FILE = Variable.get('crick_ftp_config_file_wells', default_var=None)
    HPC_SEQRUN_BASE_PATH = Variable.get('hpc_seqrun_path', default_var=None)
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


def warn_exisitng_seqrun_paths(seqrun_id, seqrun_base_dir, clean_up_tar=False):
  try:
      seqrun_path = \
        os.path.join(seqrun_base_dir, seqrun_id)
      seqrun_tar_file = \
        os.path.join(seqrun_base_dir, '{0}.tar.gz'.format(seqrun_id))
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
    logging.error(e)
    raise ValueError('Error: {0}'.format(e))


def background_handle(ftps, remote_path, local_path):
  try:
    total_size = ftps.size(remote_path)
    file_to_write = open(local_path, 'wb')
    sock = ftps.transfercmd('RETR ' + remote_path)
    while True:
        block = sock.recv(1024*1024)
        if not block:
            file_to_write.close()
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
  except Exception as e:
    raise ValueError(e)


def transfer_seqrun_tar_from_crick_ftp(
      ftp_host, ftp_conf_file, seqrun_id, seqrun_base_dir):
  try:
    if seqrun_id is None or \
       seqrun_id == "":
      raise ValueError('Missing valid seqrun id')
    logging.warn('Seqrun: {0}'.format(seqrun_id))
    warn_exisitng_seqrun_paths(
      seqrun_id=seqrun_id,
      seqrun_base_dir=seqrun_base_dir,
      clean_up_tar=True)
    ftp_conf = \
      read_json_data(ftp_conf_file)[0]
    ftps = FTP_TLS(timeout=600)
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
        temp_dir,
        '{0}.tar.gz'.format(seqrun_id))
    ftp_file_size = 0
    seqrun_tmp_file = None
    counter = 0
    for f in ftp_files:
      logging.warn('found run {0}'.format(f))
      if f == seqrun_tar_file:
        counter += 1
        #ftp_file_size = \
        #  int(ftps.size('/users/{0}/runs/{1}'.format(ftp_conf.get('username'), f)))
        #with open(temp_seqrun_tar_file, 'wb') as fp:
        #  ftps.retrbinary(
        #    'RETR /users/{0}/runs/{1}'.\
        #      format(ftp_conf.get('username'), f),
        #    fp.write)
        t = \
          threading.\
            Thread(
              target=background_handle,
              args=(
                ftps,
                '/users/{0}/runs/{1}'.format(ftp_conf.get('username'), f),
                temp_seqrun_tar_file))
        t.start()
        while t.is_alive():
          t.join(400)
          try:
            ftps.voidcmd('NOOP')
          except error_temp as e:
            logging.warn(e)
            pass
        logging.warn('downloaded tar {0}'.format(temp_seqrun_tar_file))
        ftps.close()
        break
    if counter == 0:
      raise ValueError('No tar file found for run {0}'.format(seqrun_id))
    else:
      copy_local_file(
        temp_seqrun_tar_file,
        seqrun_tar_file_path)
      logging.warn('Copied tar to {0}'.format(seqrun_tar_file_path))
      file_size = \
        os.stat(seqrun_tmp_file).st_size
      if file_size != ftp_file_size:
        raise ValueError('FTP file size and local file size are not same')
  except Exception as e:
    logging.error(e)
    raise ValueError('Error: {0}'.format(e))