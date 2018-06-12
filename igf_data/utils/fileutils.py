#!/usr/bin/env python
import pandas as pd
import os,subprocess,hashlib,string,re
import tarfile,fnmatch
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


def prepare_file_archive(results_dirpath,output_file,gzip_output=True,
                         exclude_list=None,force=True):
  '''
  A method for creating tar.gz archive with the files present in filepath
  
  :param results_dirpath: A file path for input file directory
  :param output_file: Name of the output archive filepath
  :param gzip_output: A toggle for creating gzip output tarfile, default True
  :param exclude_list: A list of file pattern to exclude from the archive, default None
  :param force: A toggle for replacing output file, if its already present, default True
  :returns: Nill
  '''
  try:
    output_mode='w'                                                             # write mode for non compressed output
    if gzip_output:
      output_mode='w:gz'                                                        # set write mode for gzip output

    if not os.path.exists(results_dirpath):
      raise IOError('Input directory path {0} not found'.\
                    format(results_dirpath))                                    # check input directory path

    if os.path.exists(output_file):
      if not force:
        raise ValueError('Output archive already present: {0},set force as True to overwrite'.\
                         format(output_file))                                   # check for existing output file
      else:
        os.remove(output_file)                                                  # removing existing file

    if not os.path.exists(os.path.dirname(output_file)):
      raise IOError('failed to write output file {0}, path not found'.\
                    format(output_file))                                        # check for existing output directory

    if exclude_list is not None and not isinstance(exclude_list,list):
      raise ValueError('Expecting a list for excluding file to archive, got {0}'.\
                       format(type(exclude_list)))                              # check exclude list type if its not None

    with tarfile.open(output_file, "{0}".format(output_mode)) as tar:           # overwrite output file
      for root,dir,files in os.walk(results_dirpath):                           # check for files under results_dirpath
        for file in files:
          file_path=os.path.join(root,
                                 file)
          if exclude_list is None:
            tar.add(file_path,
                    arcname=os.path.relpath(file_path,
                                            start=results_dirpath))             # add all files to archive
          else:
            exclude_flag=[exclude_pattern 
                            for exclude_pattern in exclude_list 
                              if fnmatch.fnmatch(file,exclude_pattern)]         # check for match with exclude pattern list
            if len(exclude_flag)==0:
              tar.add(file_path,
                      arcname=os.path.relpath(file_path,
                                              start=results_dirpath))           # add files to archive if its not in the list
  except:
    raise

def _get_file_manifest_info(file_path,start_dir=None,md5_label='md5',
                            size_lavel='size',path_label='file_path'):
  '''
  An internal method for calculating file manifest information for an input file
  
  :param file_path: A file path for manifest information generation
  :param start_dir: A directory path for generating relative filepath info, default None
  :param md5_label: A string for checksum column, default md5
  :param size_lavel: A string for file size column, default size
  :param path_label: A string for file path column, default file_path
  :returns: A dictionary with the path_label,md5_label and size_lavel as the key
  '''
  try:
    if not os.path.exists(file_path):
      raise IOError('manifest generation has failed for file {0}'.\
                    format(file_path))

    file_data=dict()
    file_relpath=os.path.relpath(file_path,
                                 start=start_dir)                               # get relative filepath
    file_md5=calculate_file_checksum(filepath=file_path,
                                    hasher='md5')                               # get file md5
    file_size=os.path.getsize(file_path)                                        # get file size
    file_data.update({path_label:file_relpath,
                      md5_label:file_md5,
                      size_lavel:file_size
                     })                                                         # update file data
    return file_data
  except:
    raise


def create_file_manifest_for_dir(results_dirpath,output_file,md5_label='md5',
                                 size_lavel='size',path_label='file_path',
                                 exclude_list=None,force=True):
  '''
  A method for creating md5 and size list for all the files in a directory path
  
  :param results_dirpath: A file path for input file directory
  :param output_file: Name of the output csv filepath
  :param exclude_list: A list of file pattern to exclude from the archive, default None
  :param force: A toggle for replacing output file, if its already present, default True
  :param md5_label: A string for checksum column, default md5
  :param size_lavel: A string for file size column, default size
  :param path_label: A string for file path column, default file_path
  :returns: Nill
  '''
  try:
    if not os.path.exists(results_dirpath):
      raise IOError('Input directory path {0} not found'.\
                    format(results_dirpath))                                    # check input directory path

    if os.path.exists(output_file):
      if not force:
        raise ValueError('Output archive already present: {0},set force as True to overwrite'.\
                         format(output_file))                                   # check for existing output file
      else:
        os.remove(output_file)                                                  # removing existing file

    if not os.path.exists(os.path.dirname(output_file)):
      raise IOError('failed to write output file {0}, path not found'.\
                    format(output_file))                                        # check for existing output directory

    if exclude_list is not None and not isinstance(exclude_list,list):
      raise ValueError('Expecting a list for excluding file to archive, got {0}'.\
                       format(type(exclude_list)))                              # check exclude list type if its not None

    file_manifest_list=list()                                                   # empty list for file manifest

    for root,dir,files in os.walk(results_dirpath):                             # check for files under results_dirpath
      for file in files:
        file_path=os.path.join(root,
                               file)
        if exclude_list is None:
          file_data=file_data=_get_file_manifest_info(\
                                file_path=file_path,
                                start_dir=results_dirpath,
                                md5_label=md5_label,
                                size_lavel=size_lavel,
                                path_label=path_label)
          file_manifest_list.append(file_data)                                  # add file info for all files
        else:
          exclude_flag=[exclude_pattern 
                          for exclude_pattern in exclude_list 
                            if fnmatch.fnmatch(file,exclude_pattern)]           # check for match with exclude pattern list
          if len(exclude_flag)==0:
            file_data=file_data=_get_file_manifest_info(\
                                  file_path=file_path,
                                  start_dir=results_dirpath,
                                  md5_label=md5_label,
                                  size_lavel=size_lavel,
                                  path_label=path_label)
            file_manifest_list.append(file_data)                                # add file info for filtered files

    file_manifest_list=pd.DataFrame(file_manifest_list)                         # convert manifest data to dataframe
    file_manifest_list.to_csv(output_file,
                              sep=',',
                              encoding='utf-8',
                              index=False)                                      # write manifest csv file
  except:
    raise
