import os
import time
import logging
from typing import Any
from ftplib import FTP_TLS
from igf_data.utils.dbutils import read_json_data

log = logging.getLogger(__name__)

def upload_file_or_dir_to_box(
      box_config_file: str,
      file_path: str,
      upload_dir: str,
      box_username: Any = None,
      skip_existing: bool = False,
      time_wait_sec: int = 2,
      box_ftp_url: str = 'ftp.box.com',
      box_size_limit: int = 34359738368) -> None:
  try:
    box_size_limit = int(box_size_limit)
    time_wait_sec = int(time_wait_sec)
    box_conf = read_json_data(box_config_file)
    # get box user name from config file if not provided
    if box_username is None:
      box_username = box_conf[0].get('box_username')
      if box_username is None:
        raise ValueError('box_username is not defined')
    # login to box and upload file
    ftps = FTP_TLS()
    ftps.connect(box_ftp_url)
    ftps.login(box_username, box_conf[0].get('box_external_pass'))
    if os.path.isdir(file_path):
      if not upload_dir.startswith('/'):
        target_dir = os.path.join('/', upload_dir)
      too_large_files = [
        f for root, _, files in os.walk(file_path)
          for f in files
            if os.stat(os.path.join(root, f)).st_size > int(box_size_limit)]
      if len(too_large_files):
        message = \
          'Too large files found, {0}'.\
            format(','.join(too_large_files))
        log.error(message)
        raise ValueError(message)
      for root, _, files in os.walk(file_path):
        for file in files:
          source_path = os.path.join(root,file)
          file_size = os.stat(source_path).st_size
          rel_path = os.path.relpath(source_path,file_path)
          target_path = \
            os.path.join(target_dir,rel_path)
          if file_size <= box_size_limit:
            dirs = [
              i for i in os.path.dirname(target_path).split('/')
                if i != '']
            output_dir = '/'
            for i in dirs:
              output_dir = os.path.join(output_dir, i)
              if i not in ftps.nlst(os.path.dirname(output_dir)):
                ftps.mkd(output_dir)
            if file in ftps.nlst(output_dir) and \
               int(ftps.size(target_path)) == file_size and \
               skip_existing:
              log.warning(f'skipping existing file {target_path}')
            else:
              ftps.storbinary(f'STOR {target_path}', open(source_path,'rb'))
              time.sleep(time_wait_sec)
    elif os.path.isfile(file_path):
      target_path = \
        os.path.join(
          upload_dir,
          os.path.basename(file_path))
      if not target_path.startswith('/'):
        target_path = f'/{target_path}'
      log.warn(f'Box file path: {target_path}')
      file_size = os.stat(file_path).st_size
      dirs = \
        [d for d in os.path.dirname(target_path).split('/')
           if d != '' and d != '/']
      log.warn(dirs)
      target_dir = '/'
      for d in dirs:
        target_dir = \
          os.path.join(target_dir, d)
        log.warn(target_dir, ftps.nlst(os.path.dirname(target_dir)))
        if d not in (ftps.nlst(os.path.dirname(target_dir))):
          ftps.mkd(os.path.join(target_dir))
      if os.path.getsize(file_path) > box_size_limit:
        message = \
          'Failed to upload large file {0}, size {1}'.\
            format(file_path, int(file_size) / 1073741824)
        log.error(message)
        raise ValueError(message)
      if os.path.basename(target_path) in ftps.nlst(target_dir) and \
         int(ftps.size(target_path)) == os.path.getsize(file_path) and \
         skip_existing:
        log.warning('skipping existing file {0}'.format(file_path))
      else:
        ftps.storbinary(f'STOR {target_path}', open(file_path, 'rb'))
      time.sleep(int(time_wait_sec))
    else:
      raise IOError(f'Not a file, {file_path}')
    ftps.close()
  except Exception as e:
    message = f'Failed box upload, error: {e}'
    log.error(message)
    raise ValueError(message)