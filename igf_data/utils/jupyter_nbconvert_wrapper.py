import os
from shutil import copytree
from datetime import datetime
from shlex import quote
from igf_data.utils.singularity_run_wrapper import singularity_run
from igf_data.utils.fileutils import get_temp_dir,remove_dir,check_file_path,copy_local_file
from jinja2 import Template,Environment, FileSystemLoader, select_autoescape

class Notebook_runner:
  '''
  A class for setup and run notebooks in containers

  :param template_ipynb_path: A template notebook file
  :param output_dir: Output path
  :param input_param_map: A dictionary containing input params and files
  :param container_dir_prefix: Container mount dir, default /tmp
  :param output_file_map: A dictionary containing output files, default None
  :param date_tag: A string tag for adding dates in the notebooks, default DATE_TAG
  :param use_ephemeral_space: A toggle for temp dir settings, default False
  :param output_format: Notebook output format, default html
  :param timeout: Timeout settings for jupyter nbconvert run, default 600
  :param kernel: Kernel name for the jupyter nbconvert run, default python3
  :param allow_errors: Allow notebook execution with errors, default False
  :param notebook_tag: A tag for dataflow to identify notebook output, default notebook
  '''
  def __init__(self,template_ipynb_path,output_dir,input_param_map,container_dir_prefix='/tmp',
               output_file_map=None,date_tag='DATE_TAG',use_ephemeral_space=False,output_format='html',
               timeout=600,kernel='python3',allow_errors=False,notebook_tag='notebook'):
    self.template_ipynb_path = template_ipynb_path
    self.output_dir = output_dir
    self.input_param_map = input_param_map
    self.date_tag = date_tag
    self.use_ephemeral_space = use_ephemeral_space
    self.output_file_map = output_file_map
    self.container_dir_prefix = container_dir_prefix
    self.output_format = output_format
    self.timeout = timeout
    self.kernel = kernel
    self.allow_errors = allow_errors
    self.notebook_tag = notebook_tag
    self.temp_dir = \
      get_temp_dir(use_ephemeral_space=self.use_ephemeral_space)


  @staticmethod
  def _copy_to_container_temp(mount_dir,container_path_prefix,filepath):
    '''
    An internal static method for copying files to container temp dir

    :param mount_dir: A dir path to mount in container
    :param container_path_prefix: Temp dir path in container
    :param filepath: File or dir path to copy
    :returns: A path in mounted temp dir and a path in the container temp dir
    '''
    try:
      check_file_path(filepath)
      container_path = \
        os.path.join(
          container_path_prefix,
          os.path.basename(filepath))
      mount_dir_path = \
        os.path.join(
          mount_dir,
          os.path.basename(filepath))
      copy_local_file(
        filepath,
        mount_dir_path,
        force=True)
      return mount_dir_path, container_path
    except Exception as e:
      raise ValueError("Failed to copy path {0} to temp dir: {1}, error: {2}".\
                        format(filepath,mount_dir,e))


  def _substitute_input_path_and_copy_files_to_tempdir(self):
    '''
    An internal method for substituting input filepaths and copying them to temp dir

    :returns: A dictionary of input param map with modified file paths
    '''
    try:
      check_file_path(self.temp_dir)
      if self.input_param_map is None or \
         not isinstance(self.input_param_map,dict):
        raise TypeError(
                "Expecting a input param dictionary and got {0}".\
                  format(type(self.input_param_map)))
      modified_input_map = dict()
      for key,entry in self.input_param_map.items():
        if isinstance(entry,str) and \
           os.path.exists(entry):
          _,new_entry = \
            self._copy_to_container_temp(
              mount_dir=self.temp_dir,
              container_path_prefix=self.container_dir_prefix,
              filepath=entry)
          modified_input_map.\
            update({key:new_entry})                                             # for filepath entry
        elif isinstance(entry,list):
          new_entry_list = list()
          for e in entry:
            if os.path.exists(e):
              _,new_entry = \
                self._copy_to_container_temp(
                  mount_dir=self.temp_dir,
                  container_path_prefix=self.container_dir_prefix,
                  filepath=e)
              new_entry_list.append(new_entry)
            else:
              new_entry_list.append(e)
          modified_input_map.\
            update({key:new_entry_list})                                        # for list entries with filepath as value
        elif isinstance(entry,dict):
          new_entry_dict = dict()
          for e_key,e_val in entry.items():
            if os.path.exists(e_val):
              _,new_entry = \
                self._copy_to_container_temp(
                  mount_dir=self.temp_dir,
                  container_path_prefix=self.container_dir_prefix,
                  filepath=e_val)
              new_entry_dict.\
                update({e_key:new_entry})
            else:
              new_entry_dict.\
                update({e_key:e_val})
            modified_input_map.\
              update({key:new_entry_dict})                                      # for dict entries with filepath as value
        else:
          modified_input_map.\
              update({key:entry})                                               # for everything else
      return modified_input_map
    except Exception as e:
      raise ValueError("Failed to modify input {0}, error: {1}".\
                         format(self.input_param_map,e))


  @staticmethod
  def _get_date_stamp():
    '''
    An internal static method for generating datestamp
    
    :returns: A string of datestampe in 'YYYY-MM-DD HH:MM' format
    '''
    try:
      date_stamp = None
      date_stamp = \
        datetime.\
          strftime(
            datetime.now(),
            '%Y-%b-%d %H:%M')                                                   # date tag values
      return date_stamp
    except Exception as e:
      raise ValueError("Failed to get datestamp, error: {0}".format(e))


  def _generate_ipynb_from_template(self,param_map):
    '''
    An internal method to generate notebook from template

    :param param_map: A dictionary for parameter substitution in output notebook
    :returns: A output notebook path
    '''
    try:
      check_file_path(self.template_ipynb_path)
      check_file_path(self.temp_dir)
      if not isinstance(param_map,dict):
        raise TypeError(
                "Expecting a dictionary for notebook param substitution, got {0}".\
                  format(type(param_map)))
      notebook_output = \
        os.path.join(
          self.temp_dir,
          os.path.basename(self.template_ipynb_path))
      template_env = \
        Environment(
          loader=\
            FileSystemLoader(
              searchpath=os.path.dirname(self.template_ipynb_path)),
          autoescape=select_autoescape(['html', 'xml']))
      notebook = \
        template_env.\
          get_template(
            os.path.basename(self.template_ipynb_path))
      notebook.\
        stream(**param_map).\
        dump(notebook_output)
      return notebook_output
    except Exception as e:
      raise ValueError("Failed to generate notebook for template: {0}, error, {1}".\
                         format(self.template_ipynb_path,e))


  def nbconvert_singularity(self,singularity_image_path,dry_run=False):
    '''
    A method for generating notebook from template and executing in singularity container

    :param singularity_image_path: A singularity image path
    :param dry_run: A toggle for dry run, default False
    :returns: A response str from singularity, run command and a dictionary of output params for dataflow
    '''
    try:
      output_params = dict()
      new_input_map = \
        self._substitute_input_path_and_copy_files_to_tempdir()                 # get modified input map and copy files to ount dir
      if not isinstance(new_input_map,dict):
        raise TypeError("Expecting a dictionary and got {0}".\
                          format(type(new_input_map)))
      date_stamp = self._get_date_stamp()                                       # get date stamp
      new_input_map.\
        update({self.date_tag:date_stamp})                                      # update input map with datestamp
      temp_notebook = \
        self._generate_ipynb_from_template(param_map=new_input_map)             # generate new notebook after param substitution
      container_notebook_path = \
        os.path.join(
          self.container_dir_prefix,
          os.path.basename(temp_notebook))
      args_list = [
        'jupyter',
        'nbconvert',
        '{0}'.format(quote(container_notebook_path)),
        '--to={0}'.format(quote(self.output_format)),
        '--execute',
        '--ExecutePreprocessor.enabled=True',
        '--ExecutePreprocessor.timeout={0}'.format(quote(str(self.timeout))),
        '--ExecutePreprocessor.kernel_name={0}'.format(quote(self.kernel))]     # prepare notebook cmd for run
      if self.allow_errors:
        args_list.append('--allow-errors')                                      # run notebooks with errors
      try:
        res = None
        res, run_cmd = \
          singularity_run(
            image_path=singularity_image_path,
            path_bind=self.temp_dir,
            use_ephemeral_space=self.use_ephemeral_space,
            args_list=args_list,
            dry_run=dry_run)                                                    # run notebook in singularity container
      except Exception as e:
        raise ValueError(
                "Failed to run jupyter command in singularity, error {0}, response: {1}".\
                  format(e,res))
      if dry_run:
        return res, run_cmd, output_params                                      # test singularity cmd
      else:
        output_params = \
          self._copy_container_output_and_update_map(
            temp_notebook_path=mount_notebook_path)                             # move files to output dir
        remove_dir(self.temp_dir)                                               # clean up temp dir
        return res, run_cmd, output_params
      
    except Exception as e:
      raise ValueError("Failed to execute notebook in singularity container, error: {0}".\
                         format(e))


  def _copy_container_output_and_update_map(self,temp_notebook_path):
    '''
    An internal method to copy output files from container output dir and update the output map dictionary

    :returns: A new dictionary with updated filepath in the values
    '''
    try:
      new_output_map=dict()
      if self.output_file_map is not None and \
         isinstance(self.output_file_map,dict):
        for key,container_path in self.output_file_map.items():
          mount_path = \
            os.path.join(
              self.temp_dir,
              os.path.basename(container_path))                                 # get output path in container mounted dir
          check_file_path(mount_path)                                           # check if its present
          final_path = \
            os.path.join(
              self.output_dir,
              os.path.basename(mount_path))                                     # get target path
          if os.path.isfile(mount_path):
            copy_local_file(
              mount_path,
              final_path)                                                       # copy file with filename
          elif os.path.isdir(mount_path):
            copy_local_file(
              mount_path,
              self.output_dir)                                                  # copy dir to target dir
          new_output_map.\
            update({key:final_path})                                            # update output map
      if self.output_format=='html':
        temp_notebook_path = \
          temp_notebook_path.replace('.ipynb','.html')
      elif self.output_format=='markdown':
        temp_notebook_path = \
          temp_notebook_path.replace('.ipynb','.md')
      elif self.output_format=='notebook':
        temp_notebook_path = temp_notebook_path
      elif self.output_format=='pdf':
        temp_notebook_path = \
          temp_notebook_path.replace('.ipynb','.pdf')
      elif self.output_format=='python':
        temp_notebook_path = \
          temp_notebook_path.replace('.ipynb','.py')
      elif self.output_format=='slide':
        temp_notebook_path = \
          temp_notebook_path.replace('.ipynb','.html')
      check_file_path(temp_notebook_path)
      output_notebook_path = \
        os.path.join(
          self.output_dir,
          os.path.basename(temp_notebook_path))
      copy_local_file(
        temp_notebook_path,
        output_notebook_path)                                                   # copy notbook file
      new_output_map.\
        update({self.notebook_tag:output_notebook_path})
      return new_output_map
    except Exception as e:
      raise ValueError(
              "Failed to copy files from container mount dir, error: {0}".\
                format(e))






def nbconvert_execute_in_singularity(image_path,ipynb_path,input_list,output_dir,output_format='html',
                                     output_file_map=None,timeout=600,kernel='python3',
                                     use_ephemeral_space=False,allow_errors=False,dry_run=False):
  '''
  A function for running jupyter nbconvert within singularity containers

  :param image_path: A singularity image path
  :param ipynb_path: A notebook file path to run in the singularity container
  :param input_list: A list of input file for notebook run
  :param output_dir: Path to copy output files
  :param output_format: Notebook output format, default html
  :param output_file_map: A a dictionary of output file tag abd name as key and value, to copy to output_path from tmp dir, default None
  :param timeout: Timeout setting for notebook execution, default 600s
  :param kernel: Kernel name for notebook execution, default python3
  :param allow_errors: A toggle for running notebook with errors, default False
  :param use_ephemeral_space: Toggle for using ephemeral space for temp dir, default False
  :param dry_run: Return the notebook command without run, default False
  :returns: notebook cmd
  '''
  try:
    check_file_path(image_path)
    check_file_path(ipynb_path)
    if output_file_map is None:
      output_file_map = dict()                                                  # default output map is an empty dictionary
    if not isinstance(input_list,list) and \
       len(input_list)==0:
       raise ValueError("Missing input files for notebook run")
    tmp_dir = get_temp_dir(use_ephemeral_space=use_ephemeral_space)             # this will be mounted on container on /tmp
    tmp_input_list = list()
    for f in input_list:
      check_file_path(f)
      temp_path = \
        os.path.join(
          tmp_dir,
          os.path.basename(f))
      copy_local_file(f,temp_path)                                              # copy input files to temp dir
      tmp_input_list.append(temp_path)
    temp_ipynb_path = \
      os.path.join(
        tmp_dir,
        os.path.basename(ipynb_path))
    copy_local_file(
      ipynb_path,
      temp_ipynb_path)                                                          # copy ipynb file to tmp dir
    args_list = [
      'jupyter',
      'nbconvert',
      '{0}'.format(quote(temp_ipynb_path)),
      '--to={0}'.format(quote(output_format)),
      '--execute',
      '--ExecutePreprocessor.enabled=True',
      '--ExecutePreprocessor.timeout={0}'.format(quote(str(timeout))),
      '--ExecutePreprocessor.kernel_name={0}'.format(quote(kernel))]            # prepare notebook cmd for run
    if allow_errors:
      args_list.append('--allow-errors')                                        # run notebooks with errors
    try:
      res = None
      res, run_cmd = \
        singularity_run(
          image_path=image_path,
          path_bind=tmp_dir,
          use_ephemeral_space=use_ephemeral_space,
          args_list=args_list,
          dry_run=dry_run)                                                      # run notebook in singularity container
    except Exception as e:
      raise ValueError("Failed to run jupyter command in singularity, error {0}, response: {1}".\
                         format(e,res))
    if output_file_map is not None and \
       isinstance(output_file_map,dict):
      for tag,output in output_file_map.items():
        output_path = output_dir
        temp_output = \
          os.path.join(
            tmp_dir,
            os.path.basename(output))                                           # just get base name
        if not dry_run:
          check_file_path(temp_output)                                          # skip output file check for dry run
        if os.path.isfile(temp_output):
          output_path = \
            os.path.join(
              output_path,
              os.path.basename(output))                                         # need file name when copying files
        if not dry_run:
          copy_local_file(
            temp_output,
            output_path)                                                        # copy file or dir to output path
        if os.path.isdir(temp_output):
          output_path = \
            os.path.join(
              output_path,
              os.path.basename(output))                                         # adding dir name to output path, once copy is over
        output_file_map.\
          update({tag:output_path})
    if output_format=='html':
      temp_ipynb_path = \
        temp_ipynb_path.replace('.ipynb','.html')
    elif output_format=='markdown':
      temp_ipynb_path = \
        temp_ipynb_path.replace('.ipynb','.md')
    elif output_format=='notebook':
      temp_ipynb_path = temp_ipynb_path
    elif output_format=='pdf':
      temp_ipynb_path = \
        temp_ipynb_path.replace('.ipynb','.pdf')
    elif output_format=='python':
      temp_ipynb_path = \
        temp_ipynb_path.replace('.ipynb','.py')
    elif output_format=='slide':
      temp_ipynb_path = \
        temp_ipynb_path.replace('.ipynb','.html')
    if not dry_run:
      check_file_path(temp_ipynb_path)                                          # check output file path
    output_ipynb_path = \
      os.path.join(
        output_dir,
        os.path.basename(temp_ipynb_path))
    if not dry_run:
      copy_local_file(
        temp_ipynb_path,
        output_ipynb_path)                                                      # copy output notebook
    output_file_map.\
      update({'notebook':output_ipynb_path})                                    # add notebook output to dataflow
    remove_dir(tmp_dir)
    return output_file_map,run_cmd
  except Exception as e:
    raise ValueError(
            "Failed to run nbconvert in singularity, error: {0}".\
              format(e))



def generate_ipynb_from_template(template_ipynb_path,output_dir,param_dictionary,date_tag='date_tag',
                                 use_ephemeral_space=False):
  '''
  A class for generating notebook IPYNB file from a template files with param substitution

  :param template_ipynb_path: A template IPYNB file path
  :param output_dir: Output path
  :param param_dictionary: A dictionary containing the params for final notebook
  :param date_tag: A text for date tag name, default date_tag
  :param use_ephemeral_space: Toggle for using ephemeral space for temp dir, default False
  :returns: None
  '''
  try:
    check_file_path(template_ipynb_path)
    check_file_path(output_dir)
    if not isinstance(param_dictionary,dict):
      raise TypeError(
              "Expecting a dictionary, got {0}".\
                format(type(param_dictionary)))
    date_tag_value = \
      datetime.\
        strftime(
          datetime.now(),
          '%Y-%b-%d %H:%M')                                                     # date tag values
    param_dictionary.\
      update(dict(date_tag=date_tag_value))                                     # adding date tag values to params
    temp_dir = \
      get_temp_dir(
        use_ephemeral_space=use_ephemeral_space)
    temp_output = \
      os.path.join(
        temp_dir,
        os.path.basename(template_ipynb_path))
    final_output = \
      os.path.join(
        output_dir,
        os.path.basename(template_ipynb_path))
    template_env = \
      Environment(
        loader=\
          FileSystemLoader(
            searchpath=os.path.dirname(template_ipynb_path)),
        autoescape=select_autoescape(['html', 'xml']))
    notebook = \
      template_env.\
        get_template(
          os.path.basename(template_ipynb_path))
    notebook.\
      stream(**param_dictionary).\
      dump(temp_output)                                                         # write temp ipynb file with param substitution
    copy_local_file(
      temp_output,
      final_output)
    remove_dir(temp_dir)
  except Exception as e:
    raise ValueError(
            "Failed to generate ipynb file from template {1}, error: {0}".\
              format(e,template_ipynb_path))

