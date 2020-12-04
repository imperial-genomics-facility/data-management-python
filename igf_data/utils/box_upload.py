import os,json,time,logging
from ftplib import FTP_TLS
from igf_data.utils.dbutils import read_json_data
from igf_data.utils.fileutils import check_file_path

def upload_file_or_dir_to_box(
      box_config_file,file_path,upload_dir,box_username,skip_existing=False,
      time_wait_sec=2,box_ftp_url='ftp.box.com',box_size_limit=34359738368):
  try:
    check_file_path(box_config_file)
    check_file_path(file_path)
    box_conf = read_json_data(box_config_file)
    box_conf = box_conf[0]
    ftps = FTP_TLS()
    ftps.connect(box_ftp_url)
    ftps.login(
      box_username,
      box_conf.get('box_external_pass'))
    target_dirs = \
      [d for d in upload_dir.split('/') if d != '']
    target_dir = ''
    for d in target_dirs:
      target_dir = \
        os.path.join(target_dir,d)
      if d not in ftps.nlst(target_dir):
        ftps.mkd(os.path.dirname(target_dir))
    if os.path.isdir(file_path):
      for root,_,files in os.walk(file_path):
        for file in files:
          source_path = os.path.join(root,file)
          file_size = os.stat(source_path).st_size
          rel_path = os.path.relpath(file_path,source_path)
          target_path = \
            os.path.join(target_dir,rel_path)
          if file_size <= box_size_limit:
            dirs = os.path.dirname(rel_path).split('/')
            for i in dirs:
              if i not in ftps.nlst(target_dir):
                ftps.mkd(os.path.join(target_dir,i))
              target_dir = os.path.join(target_dir,i)
            if file in ftps.nlst(target_dir):
              ftp_file_size = \
                ftps.size(os.path.join(target_dir,file))
              if not skip_existing and \
                 ftp_file_size != file_size:
                ftps.storbinary('STOR {0}'.format(target_path),open(source_path,'rb'))
                time.sleep(time_wait_sec)
              else:
                logging.\
                  warning('Skipped existing file {0}'.format(target_path))
            else:
              ftps.storbinary('STOR {0}'.format(target_path),open(source_path,'rb'))
              time.sleep(int(time_wait_sec))
          else:
            logging.\
              error('Failed to upload file {0}, size {1}'.\
                      format(source_path,int(file_size) / 1073741824))
    elif os.path.isfile(file_path):
      target_path = \
        os.path.join(
          upload_dir,
          os.path.basename(file_path))
      if os.path.getsize(file_path) > box_size_limit:
        message = \
          'Failed to upload file {0}, size {1}'.\
            format(source_path,int(file_size) / 1073741824)
        logging.error(message)
        raise ValueError(message)
      ftps.storbinary('STOR {0}'.format(target_path),open(file_path,'rb'))
      time.sleep(int(time_wait_sec))
    else:
      raise IOError('Not a file, {0}'.format(file_path))
    ftps.close()
  except Exception as e:
    raise ValueError('Failed box upload, error: {0}'.format(e))