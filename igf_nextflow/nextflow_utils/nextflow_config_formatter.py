import os
from igf_data.utils.fileutils import check_file_path
from igf_data.utils.fileutils import get_temp_dir
from igf_data.utils.fileutils import remove_dir
from igf_data.utils.fileutils import copy_local_file
from jinja2 import Environment,FileSystemLoader,select_autoescape

def format_nextflow_config_file(
      template_file,output_file,bind_dir_list=None,
      hpc_queue_name='',force_update=True):
  '''
  A function for generating project specific Nextflow config file

  :param template_file: A Jinja template for Nextflow config
  :param output_file: Output config file path
  :param bind_dir_list: A list of dirs to bind to singularity container, default None
  :param hpc_queue_name: HPC queue name, default empty string
  :param force_update: Overwrite the existing config file, default True
  :returns: None
  '''
  try:
    check_file_path(template_file)
    temp_dir = get_temp_dir()
    if not isinstance(bind_dir_list,list) or \
       len(bind_dir_list)==0:
      raise ValueError('No dir list found to bind in singularity container')
    temp_output_file = \
      os.path.join(temp_dir,os.path.basename(output_file))
    template_env = \
      Environment(
        loader=\
          FileSystemLoader(
            searchpath=os.path.dirname(template_file)),
        autoescape=select_autoescape(['html', 'xml']))
    nextflow_conf = \
      template_env.\
        get_template(
          os.path.basename(template_file))
    nextflow_conf.\
      stream(
        HPC_QUEUE=hpc_queue_name,
        DIR_LIST=','.join(bind_dir_list)).\
      dump(temp_output_file)
    copy_local_file(
      temp_output_file,
      output_file,
      force=force_update)
    remove_dir(temp_dir)
  except Exception as e:
    raise ValueError(
        'Failed to generate nextflow config, error: {0}'.format(e))