import os
from shutil import copytree
from datetime import datetime
from shlex import quote
from igf_data.utils.singularity_run_wrapper import singularity_run
from igf_data.utils.fileutils import get_temp_dir,remove_dir,check_file_path,copy_local_file
from jinja2 import Template,Environment,FileSystemLoader,select_autoescape

class Notebook_runner:
  '''
  A class for setup and run notebooks in containers

  :param template_ipynb_path: A template notebook file
  :param output_dir: Output path
  :param input_param_map: A dictionary containing input params and files
  :param use_ephemeral_space: A toggle for temp dir settings, default False
  :param singularity_image_path: Path to singularity image, default None
  :param output_format: Notebook output format, default html
  :param timeout: Timeout settings for jupyter nbconvert run, default 600
  :param kernel: Kernel name for the jupyter nbconvert run, default python3
  :param allow_errors: Allow notebook execution with errors, default False
  :param dry_run: A toggle for test, default False
  :param jupyter_exe: Jupyter exe path, default jupyter
  :param notebook_tag: A tag for dataflow to identify notebook output, default notebook
  '''
  def __init__(self,template_ipynb_path,output_dir,input_param_map,container_paths=None,
               singularity_image_path=None,use_ephemeral_space=False,output_format='html',
               timeout=600,kernel='python3',allow_errors=False,jupyter_exe='jupyter',dry_run=False):
    self.template_ipynb_path = template_ipynb_path
    self.output_dir = output_dir
    self.input_param_map = input_param_map
    self.use_ephemeral_space = use_ephemeral_space
    self.container_paths = container_paths
    self.singularity_image_path = singularity_image_path
    self.output_format = output_format
    self.timeout = timeout
    self.kernel = kernel
    self.allow_errors = allow_errors
    self.dry_run = dry_run
    self.jupyter_exe = jupyter_exe
    self.temp_dir = \
      get_temp_dir(use_ephemeral_space=self.use_ephemeral_space)

  def _generate_formatted_notebook(self):
    try:
      check_file_path(self.template_ipynb_path)
      check_file_path(self.temp_dir)
      if not isinstance(self.input_param_map,dict):
        raise TypeError(
                "Expecting a dictionary for notebook param substitution, got {0}".\
                  format(type(self.input_param_map)))
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
        stream(**self.input_param_map).\
        dump(notebook_output)
      return notebook_output
    except:
      raise


  def execute_notebook_in_singularity(self):
    '''
    A method for executing formatted notebooks within singularity container

    :returns: Output notebook path
    '''
    try:
      if self.singularity_image_path is None:
        raise ValueError('No valid image path found')
      check_file_path(self.singularity_image_path)
      formatted_notebook = \
        self._generate_formatted_notebook()
      args_list = [
        self.jupyter_exe,
        'nbconvert',
        '{0}'.format(quote(formatted_notebook)),
        '--to={0}'.format(quote(self.output_format)),
        '--execute',
        '--ExecutePreprocessor.enabled=True',
        '--ExecutePreprocessor.timeout={0}'.format(quote(str(self.timeout))),
        '--ExecutePreprocessor.kernel_name={0}'.format(quote(self.kernel))]
      if self.allow_errors:
        args_list.append('--allow-errors')
      container_paths = self.container_paths
      container_tmp_dir = \
        get_temp_dir(use_ephemeral_space=self.use_ephemeral_space)
      if isinstance(container_paths,list):
        container_paths.\
          extend(['{0}:/tmp'.format(container_tmp_dir),self.temp_dir])
      if container_paths is None:
        container_paths = \
          ['{0}:/tmp'.format(container_tmp_dir),self.temp_dir]
      cmd = singularity_run(
            image_path=self.singularity_image_path,
            args_list=args_list,
            bind_dir_list=container_paths,
            dry_run=self.dry_run)
      temp_notebook_path = \
        formatted_notebook
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
      if self.dry_run:
        return formatted_notebook,cmd
      else:
        check_file_path(temp_notebook_path)
        output_notebook_path = \
          os.path.join(
            self.output_dir,
            os.path.basename(temp_notebook_path))
        copy_local_file(
          temp_notebook_path,
          output_notebook_path)
        return output_notebook_path,cmd
    except Exception as e:
      raise ValueError(
              'Failed to execute notebook, error: {0}'.format(e))