#!/usr/bin/env python
import os,subprocess,hashlib,string,re
from datetime import datetime
from tempfile import mkdtemp,gettempdir
from shutil import rmtree, move, copy2

def move_file(source_path,destinationa_path, force=False):
  '''
  A method for moving files to local disk
  
  :param source_path: A source file path
  :param destination_path: A destination file path, including the file name
  :param force: Optional, set True to overwrite existing
                destination file, default is False
  '''
  try:
    if not os.path.exists(source_path):
      raise IOError('source file {0} not found'.format(source_path))

    if os.path.exists(destinationa_path) and not force:
      raise IOError('destination file {0} already present. set option "force" as True to overwrite it'.format(destinationa_path))

    dir_path=os.path.dirname(destinationa_path)
    if not os.path.exists(dir_path):
      os.makedirs(dir_path, mode=0o770)

    move(source_path, destinationa_path, copy_function=copy2)
  except:
    raise


def copy_local_file(source_path,destinationa_path, force=False):
  '''
  A method for copy files to local disk

  :param source_path: A source file path
  :param destination_path: A destination file path, including the file name
  :param force: Optional, set True to overwrite existing
                destination file, default is False
  '''
  try:
    if not os.path.exists(source_path):
      raise IOError('source file {0} not found'.format(source_path))

    if os.path.exists(destinationa_path) and not force:
      raise IOError('destination file {0} already present. set option "force" as True to overwrite it'.format(destinationa_path))

    dir_path=os.path.dirname(destinationa_path)
    if not os.path.exists(dir_path):
      os.makedirs(dir_path, mode=0o770)

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
          if proc.returncode == 255:
            raise ValueError('Error while copying file to remote server {0}:{1}'.\
                             format('Error while copying file to remote server ssh_exchange_identification',
                                    'Connection closed by remote host'))
          else:
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

def get_datestamp_label():
  '''
  A method for fetching datestamp
  :returns: A padded string of format YYYYMMDD
  '''
  try:
    time_tuple=datetime.now().timetuple()
    datestamp='{0}{1:02d}{2:02d}'.format(time_tuple.tm_year,
                                         time_tuple.tm_mon,
                                         time_tuple.tm_mday)
    return datestamp
  except:
    raise

def preprocess_path_name(input_path):
  '''
  A method for processing a filepath. It takes a file path or dirpath and
  returns the same path after removing any whitespace or ascii symbols from the input.
  
  :param path: An input file path or directory path
  :returns: A reformatted filepath or dirpath
  '''
  try:
    symbols=string.punctuation                                                  # get all punctuation chars
    symbols=symbols.replace('_','').\
                    replace('-','',).\
                    replace('.','')                                             # remove allowed characters
    sub_patterns=[(r'[{0}]'.format(symbols),'_'),
                  (r'\s+','_'),
                  (r'_+','_'),
                  (r'^_',''),
                  (r'_$','')]                                                   # compile list of patterns
    output=list()
    for path in input_path.split('/'):
      for old,new in sub_patterns:
        path=re.sub(old,new,path)
      output.append(path)                                                       # substitute from list of patterns
    output_path='/'.join(output)                                                # create output path
    return output_path
  except:
    raise

def get_file_extension(input_file):
  '''
  A method for extracting file suffix information
  
  :param input_file: A filepath for getting suffix
  :returns: A suffix string or an empty string if no suffix found
  '''
  try:
    output_suffix='.'.join(os.path.basename(input_file).\
                           split('.')[1:])                                      # get complete suffix for input file path
    return output_suffix
  except:
    raise
