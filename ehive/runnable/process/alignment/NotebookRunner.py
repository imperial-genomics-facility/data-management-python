import os
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.fileutils import get_temp_dir,remove_dir,check_file_path
from igf_data.utils.jupyter_nbconvert_wrapper import Notebook_runner

class NotebookRunner(IGFBaseProcess):
  '''
  A runnable class for running jupyter notebook based analysis
  '''
  def param_defaults(self):
    params_dict=super(NotebookRunner,self).param_defaults()
    params_dict.update({
      'sample_igf_id':None,
      'experiment_igf_id':None,
      'date_tag':'date_tag',
      'input_param_map':{},
      'output_param_map':{},
      'use_ephemeral_space':0,
      'kernel':'python3',
      'timeout':600,
      'output_format':'html',
      'allow_errors':0,
      'container_dir_prefix':'/tmp',
      'notebook_tag':'notebook',
    })
    return params_dict

  def run(self):
    '''
    A method for running the notebook based analysis in ehive

    :param project_igf_id: A project tag
    :param sample_igf_id: A sample tag, default None
    :param experiment_igf_id: An experiment tag, default None
    :param singularity_image_path: Singularity image path
    :param input_param_map: A dictionary containing input param substitution for the notebook template
    :param output_param_map: A dictionary containing the expected output file list
    :param notebook_template: Notebook template path
    :param base_work_dir: Base work dir path
    :param use_ephemeral_space: A toggle for temp dir settings
    :param input_list: A list of input files to copy to the temp dir for the notebook run. File names can also be added to input_param_map
    :param date_tag: A text tag for date_tag, default 'date_tag'
    :param kernel: Notebook kernel name, default is 'python3'
    :param timeout: Timeout setting for notebook run, default 600
    :param output_format: Notebook output format, default 'html'
    :param allow_errors: Allow notebook run with errors, default 0
    :param container_dir_prefix: target dir in container, default /tmp
    :param notebook_tag: Notebook output tag for dataflow param, default notebook
    '''
    try:
      project_igf_id = self.param_required('project_igf_id')
      sample_igf_id = self.param('sample_igf_id')
      experiment_igf_id = self.param('experiment_igf_id')
      singularity_image_path = self.param_required('singularity_image_path')
      input_param_map = self.param_required('input_param_map')
      output_param_map = self.param_required('output_param_map')
      notebook_template = self.param_required('notebook_template')
      base_work_dir = self.param_required('base_work_dir')
      use_ephemeral_space = self.param('use_ephemeral_space')
      date_tag = self.param('date_tag')
      kernel = self.param('kernel')
      timeout = self.param('timeout')
      output_format = self.param('output_format')
      allow_errors = self.param('allow_errors')
      notebook_tag = self.param('notebook_tag')
      container_dir_prefix = self.param('container_dir_prefix')
      if input_param_map is not None and \
         not isinstance(input_param_map,dict):
        raise ValueError(
                "Expecting a dictionary as input_param_map, got {0}".\
                  format(type(input_param_map)))                                  # checking input param dictionary
      if output_param_map is not None and \
         not isinstance(output_param_map,dict):
        raise ValueError(
                "Expecting a dictionary as output_param_map, got {0}".\
                  format(type(output_param_map)))                                 # checking output param dictionary
      work_dir_prefix_list = [
        base_work_dir,
        project_igf_id]
      if sample_igf_id is not None:
        work_dir_prefix_list.\
          append(sample_igf_id)
      if experiment_igf_id is not None:
        work_dir_prefix_list.\
          append(experiment_igf_id)
      work_dir_prefix = \
        os.path.join(*work_dir_prefix_list)
      work_dir = \
        self.get_job_work_dir(
          work_dir=work_dir_prefix)                                               # get a run work dir
      try:
        res = None
        run_cmd = None
        data_flow_param_dict = dict()
        nr = \
          Notebook_runner(
            template_ipynb_path=notebook_template,
            output_dir=work_dir,
            input_param_map=input_param_map,
            container_dir_prefix=container_dir_prefix,
            output_file_map=output_param_map,
            date_tag=date_tag,
            use_ephemeral_space=use_ephemeral_space,
            output_format=output_format,
            timeout=timeout,
            kernel=kernel,
            allow_errors=allow_errors,
            notebook_tag=notebook_tag)
        res, run_cmd, data_flow_param_dict = \
          nr.nbconvert_singularity(
            singularity_image_path=singularity_image_path)
      except Exception as e:
        raise ValueError(
                "Failed to run notebook, response: {0}, command: {1}, error: {2}".\
                   format(res,run_cmd,e))
      self.param(
        'dataflow_params',
        data_flow_param_dict)                                                     # update dataflow
    except Exception as e:
      message = \
        'project: {2}, sample:{3}, Error in {0}: {1}'.\
          format(
            self.__class__.__name__,
            e,
            project_igf_id,
            sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      self.post_message_to_ms_team(
          message=message,
          reaction='fail')
      raise

