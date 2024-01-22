import os
from shutil import copytree
from datetime import datetime
from shlex import quote
from typing import Union, Optional, Tuple
from igf_data.utils.singularity_run_wrapper import (
  singularity_run,
  execute_singuarity_cmd)
from igf_data.utils.fileutils import (
  get_temp_dir,
  remove_dir,
  check_file_path,
  copy_local_file)
from jinja2 import (
  Template,
  Environment,
  FileSystemLoader,
  select_autoescape)

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
  :param singularity_options: A list of singularity options, default None
  :param no_input: Add '-no-input' flag to nbconvert run, default False
  :param notebook_tag: A tag for dataflow to identify notebook output, default notebook
  '''
  def __init__(
    self,
    template_ipynb_path: str,
    output_dir: str,
    input_param_map: dict,
    container_paths: Optional[str] = None,
    singularity_image_path : Optional[str] = None,
    use_ephemeral_space: bool = False,
    output_format: str = 'html',
    singularity_options: Optional[list] = None,
    timeout: int = 600,
    kernel: str = 'python3',
    allow_errors: bool = False,
    jupyter_exe: str = 'jupyter',
    no_input: bool = False,
    use_singularity_execute: bool = False,
    dry_run: bool = False) -> None:
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
    self.singularity_options = singularity_options
    self.no_input = no_input
    self.use_singularity_execute = use_singularity_execute
    self.temp_dir = \
      get_temp_dir(
        use_ephemeral_space=self.use_ephemeral_space)

  def _generate_formatted_notebook(self) -> str:
    try:
      check_file_path(self.template_ipynb_path)
      check_file_path(self.temp_dir)
      if not isinstance(self.input_param_map, dict):
        raise TypeError(
          f"Expecting a dictionary for notebook param substitution, got {type(self.input_param_map)}")
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


  def execute_notebook_in_singularity(self) -> Tuple[str, str]:
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
        f'{quote(formatted_notebook)}',
        f'--to={quote(self.output_format)}',
        '--execute',
        '--ExecutePreprocessor.enabled=True',
        f'--ExecutePreprocessor.timeout={quote(str(self.timeout))}',
        f'--ExecutePreprocessor.kernel_name={quote(self.kernel)}']
      if self.allow_errors:
        args_list.append('--allow-errors')
      if self.no_input:
        args_list.append('--no-input')
      container_paths = self.container_paths
      container_tmp_dir = \
        get_temp_dir(
          use_ephemeral_space=self.use_ephemeral_space)
      if isinstance(container_paths, list):
        container_paths.\
          extend([
            f'{container_tmp_dir}:/tmp',
            self.temp_dir])
      if container_paths is None:
        container_paths = [
          f'{container_tmp_dir}:/tmp',
          self.temp_dir]
      if self.use_singularity_execute:
        cmd = \
        execute_singuarity_cmd(
          image_path=self.singularity_image_path,
          command_string=" ".join(args_list),
          options=self.singularity_options,
          bind_dir_list=container_paths,
          dry_run=self.dry_run)
      else:
        cmd = \
          singularity_run(
            image_path=self.singularity_image_path,
            args_list=args_list,
            options=self.singularity_options,
            bind_dir_list=container_paths,
            dry_run=self.dry_run)
      temp_notebook_path = \
        formatted_notebook
      if self.output_format=='html':
        temp_notebook_path = \
          temp_notebook_path.replace('.ipynb', '.html')
      elif self.output_format=='markdown':
        temp_notebook_path = \
          temp_notebook_path.replace('.ipynb', '.md')
      elif self.output_format=='notebook':
        temp_notebook_path = temp_notebook_path
      elif self.output_format=='pdf':
        temp_notebook_path = \
          temp_notebook_path.replace('.ipynb', '.pdf')
      elif self.output_format=='python':
        temp_notebook_path = \
          temp_notebook_path.replace('.ipynb', '.py')
      elif self.output_format=='slide':
        temp_notebook_path = \
          temp_notebook_path.replace('.ipynb', '.html')
      if self.dry_run:
        return formatted_notebook, cmd
      else:
        check_file_path(temp_notebook_path)
        output_notebook_path = \
          os.path.join(
            self.output_dir,
            os.path.basename(temp_notebook_path))
        copy_local_file(
          temp_notebook_path,
          output_notebook_path)
        return output_notebook_path, cmd
    except Exception as e:
      raise ValueError(
        f'Failed to execute notebook, error: {e}')