#!/usr/bin/env python
import os, subprocess, hashlib
from tempfile import mkdtemp,gettempdir
from shutil import rmtree, move, copy2

def move_file(source_path,destinationa_path, force=False):
    '''
    A method for moving files to local disk
    required params:
    source_path: A source file path
    destination_path: A destination file path
    force: Optional, set True to overwrite existing
           destination file, default is False
    '''
    try:
        if not os.path.exists(source_path):
            raise IOError('source file {0} not found'.format(source_path))
        if os.path.exists(destinationa_path) and not force:
            raise IOError('destination file {0} already present. set option "force" as True to overwrite it'.format(destinationa_path))
        move(source_path, destinationa_path, copy_function=copy2)
    except:
        raise


def copy_local_file(source_path,destinationa_path, force=False):
    '''
    A method for copy files to local disk
    required params:
    source_path: A source file path
    destination_path: A destination file path
    force: Optional, set True to overwrite existing
           destination file, default is False
    '''
    try:
        if not os.path.exists(source_path):
            raise IOError('source file {0} not found'.format(source_path))
        if os.path.exists(destinationa_path) and not force:
            raise IOError('destination file {0} already present. set option "force" as True to overwrite it'.format(destinationa_path))
        copy2(source_path, destinationa_path, follow_symlinks=True)
    except:
        raise


def copy_remote_file(source_path,destinationa_path, source_address=None, destination_address=None, copy_method='rsync',check_file=True):
    '''
    A method for copy files from or to remote location
    required params:
    source_path: A source file path
    destination_path: A destination file path
    source_address: Address of the source server
    destination_address: Address of the destination server
    copy_method: A nethod for copy files, default is 'rsync'
    check_file: Check file after transfer using checksum, default True
    '''
    try:
        if source_address is None and destination_address is None:
            raise ValueError('required a remote source address or a destination address')

        if source_address is not None:
            source_path='{0}:{1}'.format(source_address,source_path)

        if destination_address is not None:
          dir_cmd=['ssh',destination_address,'mkdir','-p',os.path.dirname(destinationa_path)]
          subprocess.check_call(dir_cmd)
          destinationa_path='{0}:{1}'.format(destination_address,destinationa_path)
        else:
          dir_cmd=['mkdir','-p',os.path.dirname(destinationa_path)]
          subprocess.check_call(dir_cmd)

        if copy_method == 'rsync':
          cmd=['rsync']
          if check_file:
            cmd.append('-c')                                                    # file check now optional
          cmd.extend(['-e','ssh',source_path,destinationa_path])
        else:
            raise ValueError('copy method {0} is not supported'.\
                             format(copy_method))

        proc=subprocess.Popen(cmd,stderr=subprocess.PIPE)
        proc.wait()
        if proc.returncode !=0:                                                 # fetching error message for non zero exits
          outs,errs=proc.communicate()
          raise ValueError('Error while copying file to remote server {0}'.\
                           format(errs.decode('utf8')))

    except:
        raise


def calculate_file_checksum(filepath, hasher='md5'):
  '''
  A method for file checksum calculation
  required param:
  filepath: a file path
  hasher: default is md5, allowed: md5 or sha256
  '''
  try:
    with open(filepath, 'rb') as infile:
      if hasher=='md5':
        file_checksum=hashlib.md5(infile.read()).hexdigest()
        return file_checksum
      elif hasher=='sha256':
        file_checksum=hashlib.sha256(infile.read()).hexdigest()
        return file_checksum
      else:
        raise ValueError('hasher {0} is not supported'.format(hasher))
  except:
    raise


def get_temp_dir(work_dir=None, prefix='temp'):
  '''
  A function for creating temp directory
  required params:
  work_dir: A path for work directory, default None
  prefix: A prefix for directory path, default 'temp'
  returns temp_dir
  '''
  try:
    if work_dir is not None and not os.path.isdir(work_dir):                    # check if work directory is present
      raise IOError('work directory {0} is not present'.format(work_dir))
    
    if work_dir is None:
      work_dir=gettempdir()

    temp_dir=mkdtemp(prefix=prefix,dir=work_dir)                                # create a temp dir
    return temp_dir
  except:
    raise


def remove_dir(dir_path):
  '''
  A function for removing directory containing files
  required params:
  dir_path: A directory path
  '''
  try:
    if not os.path.isdir(dir_path):
      raise IOError('directory path {0} is not present'.format(dir_path))
    rmtree(dir_path)
  except:
    raise