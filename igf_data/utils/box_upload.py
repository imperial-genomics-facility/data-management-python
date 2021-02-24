import os,json,time,logging
from ftplib import FTP_TLS
from igf_data.utils.dbutils import read_json_data
from igf_data.utils.fileutils import check_file_path

 def upload_file_or_dir_to_box(
      box_config_file,file_path,upload_dir,box_username,skip_existing=False,
      time_wait_sec=2,box_ftp_url='ftp.box.com',box_size_limit=34359738368):
  try:
    box_size_limit = int(box_size_limit)
    time_wait_sec = int(time_wait_sec)
    box_conf = read_json_data(box_config_file)
    ftps = FTP_TLS()
    ftps.connect(box_ftp_url)
    ftps.login(box_username,box_conf[0].get('box_external_pass'))
    target_dir = os.path.join('/',upload_dir)
    if os.path.isdir(file_path):
      too_large_files = [
        f for root,_,files in os.walk(file_path)
          for f in files
            if os.stat(os.path.join(root,f)).st_size > int(box_size_limit)]
      if len(too_large_files):
        message = \
          'Too large files found, {0}'.\
            format(','.join(too_large_files))
        logging.error(message)
        raise ValueError(message)
      for root,_,files in os.walk(file_path):
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
              output_dir = os.path.join(output_dir,i)
              if i not in ftps.nlst(os.path.dirname(output_dir)):
                ftps.mkd(output_dir)
            if file in ftps.nlst(output_dir) and \
               int(ftps.size(target_path)) == file_size and \
               skip_existing:
              logging.warning('skipping existing file {0}'.format(target_path))
            else:
              ftps.storbinary('STOR {0}'.format(target_path),open(source_path,'rb'))
              time.sleep(time_wait_sec)
    elif os.path.isfile(file_path):
      target_path = \
        os.path.join(
          upload_dir,
          os.path.basename(file_path))
      dirs = \
        [d for d in os.path.dirname(target_path).split('/')
           if d != '']
      target_dir = '/'
      for d in dirs:
        target_dir = \
          os.path.join(target_dir,d)
        if d not in (ftps.nlst(os.path.dirname(target_dir))):
          ftps.mkd(target_dir)
      if os.path.getsize(file_path) > box_size_limit:
        message = \
          'Failed to upload large file {0}, size {1}'.\
            format(source_path,int(file_size) / 1073741824)
        logging.error(message)
        raise ValueError(message)
      if os.path.basename(target_path) in ftps.nlst(target_dir) and \
         int(ftps.size(target_path)) == os.path.getsize(file_path) and \
         skip_existing:
        logging.warning('skipping existing file {0}'.format(file_path))
      else:
        ftps.storbinary('STOR {0}'.format(target_path),open(file_path,'rb'))
      time.sleep(int(time_wait_sec))
    else:
      raise IOError('Not a file, {0}'.format(file_path))
    ftps.close()
  except Exception as e:
    message = 'Failed box upload, error: {0}'.format(e)
    logging.error(message)
    raise ValueError(message)