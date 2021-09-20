import os, logging
from ftplib import FTP_TLS
from igf_data.utils.fileutils import read_json_data

def warn_exisitng_seqrun_paths(seqrun_id, seqrun_base_dir):
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
        raise ValueError(
                'tar file for seqrun {0} already present'.\
                  format(seqrun_id))
  except Exception as e:
    logging.error(e)
    raise ValueError('Error: {0}'.format(e))


def transfer_seqrun_tar_from_crick_ftp(
      ftp_host, ftp_conf_file, seqrun_id, seqrun_base_dir):
  try:
    if seqrun_id is None or \
       seqrun_id == "":
      raise ValueError('Missing valid seqrun id')
    logging.warn('Seqrun: {0}'.format(seqrun_id))
    warn_exisitng_seqrun_paths(
      seqrun_id=seqrun_id,
      seqrun_base_dir=seqrun_base_dir)
    ftp_conf = \
        read_json_data(ftp_conf_file)[0]
    ftps = FTP_TLS()
    ftps.connect(ftp_host)
    ftps.login(
      ftp_conf.get('username'),
      ftp_conf.get('password'))
    ftp_files = \
        ftps.nlst('/users/{0}/runs'.format(ftp_conf.get('username')))
    seqrun_tar_file = \
        '{0}.tar.gz'.format(seqrun_id)
    for f in ftp_files:
      logging.warn('found run {0}'.format(f))
      if f == seqrun_tar_file:
        seqrun_tmp_file = \
          os.path.join(seqrun_base_dir, seqrun_tar_file)
        with open(seqrun_tmp_file, 'wb') as fp:
          ftps.retrbinary(
            'RETR /users/{0}/runs/{1}'.\
              format(ftp_conf.get('username'), f),
            fp.write)
        logging.warn('downloaded tar {0}'.format(seqrun_tar_file))
    ftps.close()
  except Exception as e:
    logging.error(e)
    raise ValueError('Error: {0}'.format(e))