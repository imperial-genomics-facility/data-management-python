import os
from shutil import copytree
from datetime import datetime
from shlex import quote
from igf_data.utils.singularity_run_wrapper import singularity_run
from igf_data.utils.fileutils import get_temp_dir,remove_dir,check_file_path,copy_local_file
from jinja2 import Template,Environment, FileSystemLoader, select_autoescape

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


def nbconvert_execute_in_singularity(image_path,ipynb_path,input_list,output_dir,output_format='html',
                                     output_file_map=None,timeout=600,kernel='python3',
                                     use_ephemeral_space=False,allow_errors=False):
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
  :returns: None
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
      '-to={0}'.format(quote(output_format)),
      '--execute',
      '--ExecutePreprocessor.enabled=True',
      '--ExecutePreprocessor.timeout={0}'.format(quote(str(timeout))),
      '--ExecutePreprocessor.kernel_name={0}'.format(quote(kernel)),
      '/tmp/{0}'.format(os.path.basename(temp_path))]                          # prepare notebook cmd for run
    if allow_errors:
      args_list.append('--allow-errors')                                        # run notebooks with errors
    singularity_run(
      image_path=image_path,
      path_bind=tmp_dir,
      use_ephemeral_space=use_ephemeral_space,
      args_list=args_list)                                                      # run notebook in singularity container
    if output_file_map is not None and \
       isinstance(output_file_map,dict):
      for tag,output in output_file_map.items():
        output_path = output_dir
        temp_output = \
          os.path.join(
            tmp_dir,
            os.path.basename(output))                                           # just get base name
        check_file_path(temp_output)
        if os.path.isfile(temp_output):
          output_path = \
            os.path.join(
              output_path,
              output)                                                           # need file name when copying files
        copy_local_file(
          temp_output,
          output_path)                                                          # copy file or dir to output path
        if os.path.isdir(temp_output):
          output_path = \
            os.path.join(
              output_path,
              output)                                                           # adding dir name to output path, once copy is over
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
    check_file_path(temp_ipynb_path)                                            # check output file path
    output_ipynb_path = \
      os.path.join(
        output_dir,
        os.path.basename(temp_ipynb_path))
    copy_local_file(
      temp_ipynb_path,
      output_ipynb_path)                                                        # copy output notebook
    output_file_map.\
      update({'notebook':output_ipynb_path})                                    # add notebook output to dataflow
    remove_dir(tmp_dir)
    return output_file_map
  except Exception as e:
    raise ValueError(
            "Failed to run nbconvert in singularity, error: {0}".\
              format(e))