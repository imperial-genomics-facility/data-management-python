#!/usr/bin/env python
import json
import pandas as pd
import os,subprocess,hashlib,string,re
import tarfile,fnmatch
from shlex import quote
from datetime import datetime
from dateutil.parser import parse
from tempfile import mkdtemp,gettempdir
from shutil import rmtree, move, copy2,copytree

def move_file(source_path,destination_path,cd_to_dest=False,force=False):
  '''
  A method for moving files to local disk

  :param source_path: A source file path
  :param destination_path: A destination file path, including the file name
  :param force: Optional, set True to overwrite existing
                destination file, default is False
  '''
  try:
    if not os.path.exists(source_path):
      raise IOError(
              'source file {0} not found'.format(source_path))
    if os.path.exists(destination_path) and not force:
      raise IOError(
              'destination file {0} already present. set option "force" as True to overwrite it'.\
                format(destination_path))
    dir_path = os.path.dirname(destination_path)
    if not os.path.exists(dir_path):
      os.makedirs(dir_path, mode=0o770)
    current_dir = os.getcwd()
    if cd_to_dest:
      os.chdir(dir_path)
    move(source_path,destination_path,copy_function=copy2)
    check_file_path(destination_path)
    if cd_to_dest:
      os.chdir(current_dir)
  except Exception as e:
    raise ValueError("Failed to move file, error: {0}".format(e))


def read_json_data(data_file):
  '''
  A method for reading data from json file

  :param data_file: A Json format file
  :returns: A list of dictionaries
  '''
  data = None
  try:
    if not os.path.exists(data_file):
      raise IOError('file {0} not found'.format(data_file))
    with open(data_file, 'r') as json_data: 
      data = json.load(json_data)
    if data is None:
      raise ValueError('No data found in file {0}'.format(data))
    if not isinstance(data, list):
      data = [data]                                                             # convert data dictionary to a list of dictionaries
    return data
  except Exception as e:
    raise ValueError(
            'Failed to copy JSON file {0}, error: {1}'.\
              format(data_file,e))


def check_file_path(file_path):
  '''
  A function for checking existing filepath

  :param file_path: An input filepath for check
  :raise IOError: It raises IOError if file not found
  '''
  try:
    if not os.path.exists(file_path):
      raise IOError(
              'Missing file path {0}'.format(file_path))
  except Exception as e:
    raise ValueError(
            "Failed to check filepath, error: {0}".format(e))


def list_remote_file_or_dirs(remote_server,remote_path,only_dirs=True,
                             only_files=False,user_name=None,user_pass=None):
  '''
  A method for listing dirs or files on the remote dir paths

  :param remote_server: Semote servet address
  :param remote_path: Path on remote server
  :param only_dirs: Toggle for listing only dirs, default True
  :param only_files: Toggle for listing only files, default False
  :param user_name: User name, default None
  :param user_pass: User pass, default None
  :returns: A list of dir or file paths
  '''
  try:
    import paramiko
    if only_dirs and only_files:
      raise ValueError('Conflicting options, only_dirs and only_files')

    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy( paramiko.AutoAddPolicy() )
    try:
      client.connect(\
        hostname=remote_server,
        port=22,
        username=user_name,
        password=user_pass)
      remote_cmd = \
        ['find',
         quote(remote_path),
         '-maxdepth 1']
      if only_dirs:
        remote_cmd.append('-type d')
      if only_files:
        remote_cmd.append('-type f')
      _, stdout, stderr = \
        client.exec_command(' '.join(remote_cmd))
      output = stdout.read().decode('utf-8')
      error = stderr.read().decode('utf-8')
      client.close()
    except Exception as e:
      client.close()
      raise ValueError(
              "Failed to connect to remote server, error: {0}".\
                format(e))
    if error != '':
      raise ValueError(
              'Remote command got following errors: {0}'.\
                format(error))
    output = output.strip().split('\n')
    output = \
      [i for i in output 
        if i != remote_path]
    return output
  except Exception as e:
    raise ValueError(
            "Failed to list files from remote server, error: {0}".\
              format(e))


def copy_local_file(source_path,destination_path,cd_to_dest=True,force=False):
  '''
  A method for copy files to local disk

  :param source_path: A source file path
  :param destination_path: A destination file path, including the file name  ##FIX TYPO
  :param cd_to_dest: Change to destination dir before copy, default True
  :param force: Optional, set True to overwrite existing
                destination file, default is False
  '''
  try:
    destination_path = destination_path                                        # NEED TO FIX TYPO
    if not os.path.exists(source_path):
      raise IOError('source file {0} not found'.\
                    format(source_path))
    dir_path = os.path.dirname(destination_path)
    if not os.path.exists(dir_path):
      os.makedirs(dir_path, mode=0o770)
    current_dir = os.getcwd()                                                   # present dir path
    if cd_to_dest:
      os.chdir(dir_path)                                                        # change to dest dir before copy
    if os.path.isfile(source_path):
      if os.path.exists(destination_path) and not force:
        raise IOError(
                'destination file {0} already present. set option "force" as True to overwrite it'.\
                  format(destination_path))
      copy2(source_path, destination_path, follow_symlinks=True)                # copy file
      check_file_path(destination_path)
    elif os.path.isdir(source_path):
      if os.path.exists(destination_path) and not force:
        raise ValueError(
                'Failed to copy dir {0}, path already exists, use force to remove it'.\
                  format(destination_path))
      if os.path.exists(destination_path) and force:
        remove_dir(destination_path)
      copytree(source_path,destination_path)                                    # copy dir
      check_file_path(destination_path)
    if cd_to_dest:
      os.chdir(current_dir)                                                     # change to original path after copy
  except Exception as e:
    raise ValueError("Failed to copy local file, error: {0}".format(e))


def copy_remote_file(
      source_path,destination_path,source_address=None,destination_address=None,
      copy_method='rsync',check_file=True, force_update=False,exclude_pattern_list=None):
    '''
    A method for copy files from or to remote location

    :param source_path: A source file path
    :param destination_path: A destination file path
    :param source_address: Address of the source server
    :param destination_address: Address of the destination server
    :param copy_method: A nethod for copy files, default is 'rsync'
    :param check_file: Check file after transfer using checksum, default True
    :param force_update: Overwrite existing file or dir, default is False
    :param exclude_pattern_list: List of file pattern to exclude, Deefault None
    '''
    try:
        if source_address is None and \
           destination_address is None:
            raise ValueError('Required a remote source address or a destination address')
        path_with_space_pattern = re.compile(r'\S+\s\S+')
        if re.match(path_with_space_pattern,source_path):
          source_path = source_path.replace(' ',r'\ ')                          # fix for space in source file path
        if source_address is not None:
            source_path = \
              '{0}:{1}'.format(
                source_address,
                source_path)
        if destination_address is not None:
          dir_cmd = [
            'ssh',destination_address,'mkdir','-p',
            os.path.dirname(destination_path)]
          subprocess.check_call(dir_cmd)
          destination_path = \
            '{0}:{1}'.format(
              destination_address,
              destination_path)
        else:
          dir_cmd = [
            'mkdir','-p',
            os.path.dirname(destination_path)]
          subprocess.check_call(dir_cmd)
        if copy_method == 'rsync':
          cmd = ['rsync']
          if check_file:
            cmd.append('-c')                                                    # file check now optional
          if force_update:
            cmd.append('-I')
          if exclude_pattern_list is not None and \
             ( isinstance(exclude_pattern_list,list) and \
               len(exclude_pattern_list)>0 ):
            for exclude_path in exclude_pattern_list:
              cmd.extend(['--exclude',quote(exclude_path)])                     # added support for exclude pattern
          cmd.extend(['-r','-p','-e','ssh',source_path,destination_path])
        else:
            raise ValueError('copy method {0} is not supported'.\
                             format(copy_method))
        proc = subprocess.Popen(cmd,stderr=subprocess.PIPE)
        proc.wait()
        if proc.returncode !=0:                                                 # fetching error message for non zero exits
          _,errs = proc.communicate()
          if proc.returncode == 255:
            raise ValueError(\
              '{0}:{1}'.\
              format('Error while copying file to remote server ssh_exchange_identification',
                     'Connection closed by remote host'))
          else:
            raise ValueError('Error while copying file to remote server {0}'.\
                             format(errs.decode('utf8')))

    except Exception as e:
        raise ValueError("Failed to copy remote file, error: {0}".format(e))


def calculate_file_checksum(filepath, hasher='md5'):
  '''
  A method for file checksum calculation

  :param filepath: a file path
  :param hasher: default is md5, allowed: md5 or sha256
  :returns: file checksum value
  '''
  try:
    with open(filepath, 'rb') as infile:
      if hasher=='md5':
        hasher_obj = hashlib.md5()
      elif hasher=='sha256':
        hasher_obj = hashlib.sha256()
      else:
        raise ValueError('hasher {0} is not supported'.format(hasher))
      while True:
        fileBuffer = infile.read(16 * 1024)
        if not fileBuffer:
          break
        hasher_obj.update(fileBuffer)
      return hasher_obj.hexdigest()
  except Exception as e:
    raise ValueError("Failed to check file checksum, error: {0}".format(e))


def get_temp_dir(work_dir=None, prefix='temp',use_ephemeral_space=False):
  '''
  A function for creating temp directory

  :param work_dir: A path for work directory, default None
  :param prefix: A prefix for directory path, default 'temp'
  :param use_ephemeral_space: Use env variable $EPHEMERAL to get work directory, default False
  :returns: A temp_dir
  '''
  try:
    if work_dir is not None and not os.path.isdir(work_dir):                    # check if work directory is present
      raise IOError('work directory {0} is not present'.format(work_dir))
    if isinstance(use_ephemeral_space, int) and \
       use_ephemeral_space == 0:
      use_ephemeral_space = False
    elif isinstance(use_ephemeral_space, int) and \
         use_ephemeral_space > 0:
      use_ephemeral_space = True
    if work_dir is None:
      if use_ephemeral_space:
        work_dir = os.environ.get('EPHEMERAL')
        if work_dir is None:
          raise ValueError(
                  'Env variable EPHEMERAL is not available, set use_ephemeral_space as False')
      else:
        work_dir=gettempdir()
    temp_dir = \
      mkdtemp(
        prefix=prefix,
        dir=work_dir)                                                           # create a temp dir
    return temp_dir
  except Exception as e:
    raise ValueError("Failed to get temp dir, error: {0}".format(e))


def remove_dir(dir_path,ignore_errors=True):
  '''
  A function for removing directory containing files

  :param dir_path: A directory path
  :param ignore_errors: Ignore errors while removing dir, default True
  '''
  try:
    if not os.path.isdir(dir_path):
      raise IOError('directory path {0} is not present'.format(dir_path))
    rmtree(dir_path,ignore_errors=ignore_errors)
  except Exception as e:
    raise ValueError("Failed to remove dir, error: {0}".format(e))


def get_datestamp_label(datetime_str=None):
  '''
  A method for fetching datestamp

  :param datetime_str: A datetime string to parse, default None
  :returns: A padded string of format YYYYMMDD
  '''
  try:
    if datetime_str is None:
      time_tuple = datetime.now().timetuple()                                   # get current date
    else:
      if isinstance(datetime_str,str):
        datetime_obj=parse(datetime_str)                                        # parse a string
      elif isinstance(datetime_str,datetime):
        datetime_obj=datetime_str                                               # check for datetime object
      else:
        raise ValueError('Expecting a datetime string and got : {0}'.\
                         format(type(datetime_str)))
      time_tuple = datetime_obj.timetuple()
    datestamp = \
      '{0}{1:02d}{2:02d}'.format(
        time_tuple.tm_year,
        time_tuple.tm_mon,
        time_tuple.tm_mday)
    return datestamp
  except Exception as e:
    raise ValueError("Failed to get datestamp label, error: {0}".format(e))


def preprocess_path_name(input_path):
  '''
  A method for processing a filepath. It takes a file path or dirpath and
  returns the same path after removing any whitespace or ascii symbols from the input.

  :param path: An input file path or directory path
  :returns: A reformatted filepath or dirpath
  '''
  try:
    symbols = string.punctuation                                                # get all punctuation chars
    symbols = \
      symbols.\
        replace('_','').\
        replace('-','',).\
        replace('.','')                                                         # remove allowed characters
    sub_patterns = [
      (r'[{0}]'.format(symbols),'_'),
      (r'\s+','_'),
      (r'_+','_'),
      (r'^_',''),
      (r'_$','')]                                                               # compile list of patterns
    output = list()
    for path in input_path.split('/'):
      for old,new in sub_patterns:
        path = re.sub(old,new,path)
      output.append(path)                                                       # substitute from list of patterns
    output_path = '/'.join(output)                                              # create output path
    return output_path
  except Exception as e:
    raise ValueError("Failed to preprocess path name, error: {0}".format(e))


def get_file_extension(input_file):
  '''
  A method for extracting file suffix information

  :param input_file: A filepath for getting suffix
  :returns: A suffix string or an empty string if no suffix found
  '''
  try:
    output_suffix = \
      '.'.join(os.path.basename(input_file).\
        split('.')[1:])                                                         # get complete suffix for input file path
    return output_suffix
  except Exception as e:
    raise ValueError("Failed to get file extension, error: {0}".format(e))


def prepare_file_archive(results_dirpath,output_file,gzip_output=True,
                         exclude_list=None,force=True,output_mode='w'):
  '''
  A method for creating tar.gz archive with the files present in filepath

  :param results_dirpath: A file path for input file directory
  :param output_file: Name of the output archive filepath
  :param gzip_output: A toggle for creating gzip output tarfile, default True
  :param exclude_list: A list of file pattern to exclude from the archive, default None
  :param force: A toggle for replacing output file, if its already present, default True
  :param output_mode: File output mode, default 'w' (or 'w:gz: for gzip files)
  :returns: None
  '''
  try:
    if gzip_output:
      output_mode='w:gz'                                                        # set write mode for gzip output
    if not os.path.exists(results_dirpath):
      raise IOError('Input directory path {0} not found'.\
                    format(results_dirpath))                                    # check input directory path
    if os.path.exists(output_file):
      if not force:
        raise ValueError(
                'Output archive already present: {0},set force as True to overwrite'.\
                  format(output_file))                                          # check for existing output file
      else:
        os.remove(output_file)                                                  # removing existing file
    if not os.path.exists(os.path.dirname(output_file)):
      raise IOError(
              'Failed to write output file {0}, path not found'.\
                format(output_file))                                            # check for existing output directory
    if exclude_list is not None and \
       not isinstance(exclude_list,list):
      raise ValueError(
              'Expecting a list for excluding file to archive, got {0}'.\
                format(type(exclude_list)))                                     # check exclude list type if its not None
    with tarfile.open(output_file, "{0}".format(output_mode)) as tar:           # overwrite output file
      for root,_,files in os.walk(results_dirpath):                             # check for files under results_dirpath
        for file in files:
          file_path = \
            os.path.join(root,file)
          if exclude_list is None:
            tar.add(
              file_path,
              arcname=\
                os.path.relpath(file_path,start=results_dirpath))               # add all files to archive
          else:
            exclude_flag = [
              exclude_pattern
                for exclude_pattern in exclude_list
                  if fnmatch.fnmatch(file,exclude_pattern)]                     # check for match with exclude pattern list
            if len(exclude_flag)==0:
              tar.add(
                file_path,
                arcname=\
                  os.path.relpath(file_path,start=results_dirpath))             # add files to archive if its not in the list
  except Exception as e:
    raise ValueError("Failed to prepare file archive, error: {0}".format(e))


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
      raise IOError(
              'manifest generation has failed for file {0}'.\
                format(file_path))
    file_data = dict()
    file_relpath = \
      os.path.relpath(file_path,start=start_dir)                                # get relative filepath
    file_md5 = \
      calculate_file_checksum(
        filepath=file_path,
        hasher='md5')                                                           # get file md5
    file_size = os.path.getsize(file_path)                                      # get file size
    file_data.update({
      path_label:file_relpath,
      md5_label:file_md5,
      size_lavel:file_size})                                                    # update file data
    return file_data
  except Exception as e:
    raise ValueError("Failed to get manifest info, error: {0}".format(e))


def create_file_manifest_for_dir(
      results_dirpath,output_file,md5_label='md5',size_lavel='size',
      path_label='file_path',exclude_list=None,force=True):
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
      raise IOError(
              'Input directory path {0} not found'.\
                format(results_dirpath))                                        # check input directory path
    if os.path.exists(output_file):
      if not force:
        raise ValueError(
                'Output archive already present: {0},set force as True to overwrite'.\
                  format(output_file))                                          # check for existing output file
      else:
        os.remove(output_file)                                                  # removing existing file
    if not os.path.exists(os.path.dirname(output_file)):
      raise IOError(
              'failed to write output file {0}, path not found'.\
                format(output_file))                                            # check for existing output directory
    if exclude_list is not None and not isinstance(exclude_list,list):
      raise ValueError(
              'Expecting a list for excluding file to archive, got {0}'.\
                format(type(exclude_list)))                                     # check exclude list type if its not None
    file_manifest_list = list()                                                   # empty list for file manifest
    for root,_,files in os.walk(results_dirpath):                             # check for files under results_dirpath
      for file in files:
        file_path = \
          os.path.join(root,file)
        if exclude_list is None:
          file_data = \
            _get_file_manifest_info(
              file_path=file_path,
              start_dir=results_dirpath,
              md5_label=md5_label,
              size_lavel=size_lavel,
              path_label=path_label)
          file_manifest_list.append(file_data)                                  # add file info for all files
        else:
          exclude_flag = [
            exclude_pattern
              for exclude_pattern in exclude_list 
                if fnmatch.fnmatch(file,exclude_pattern)]                       # check for match with exclude pattern list
          if len(exclude_flag)==0:
            file_data = \
              _get_file_manifest_info(
                file_path=file_path,
                start_dir=results_dirpath,
                md5_label=md5_label,
                size_lavel=size_lavel,
                path_label=path_label)
            file_manifest_list.append(file_data)                                # add file info for filtered files
    file_manifest_list = pd.DataFrame(file_manifest_list)                       # convert manifest data to dataframe
    file_manifest_list.to_csv(
      output_file,
      sep=',',
      encoding='utf-8',
      index=False)                                                              # write manifest csv file
  except Exception as e:
    raise ValueError("Failed to create manifest file, error: {0}".format(e))


def get_date_stamp():
  '''
  A method for generating datestamp for files

  :returns: A string of datestampe in 'YYYY-MM-DD HH:MM' format
  '''
  try:
    date_stamp = None
    date_stamp = \
      datetime.\
        strftime(
          datetime.now(),
          '%Y-%b-%d %H:%M')                                                     # date tag values
    return date_stamp
  except Exception as e:
    raise ValueError("Failed to get datestamp, error: {0}".format(e))