import os
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.fileutils import get_temp_dir,remove_dir,check_file_path
from igf_data.utils.jupyter_nbconvert_wrapper import generate_ipynb_from_template,nbconvert_execute_in_singularity

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
      'output_format':'html,'
      'allow_errors':0,
    })
    return params_dict

  def run(self):
    '''
    A method for running the analysis
    '''
  try:
    project_igf_id = self.param_required('project_igf_id')
    sample_igf_id = self.param('sample_igf_id')
    experiment_igf_id = self.param('experiment_igf_id')
    singularity_image_path = self.param_required('singularity_image_path')
    command_list = self.param_required(command_list)
    input_param_map = self.param_required('input_param_map')
    output_param_map = self.param_required('output_param_map')
    notebook_template = self.param_required('notebook_template')
    base_work_dir = self.param_required('base_work_dir')
    use_ephemeral_space = self.param('use_ephemeral_space')
    input_list = self.param_required('input_list')
    date_tag = self.param('date_tag')
    kernel = self.param('kernel')
    timeout = self.param('timeout')
    output_format = self.param('output_format')
    allow_errors = self.param('allow_errors')
    work_dir_prefix_list = [
      base_work_dir,
      project_igf_id]
    if sample_igf_id is not None:
      work_dir_prefix_list.\
        append(sample_igf_id)
    if experiment_igf_id is not None:
      work_dir_prefix_list.\
        append(experiment_igf_id)

    output_file_list = output_param_map.values()
    work_dir_prefix = \
      os.path.join(work_dir_prefix_list)
    work_dir = \
      self.get_job_work_dir(
        work_dir=work_dir_prefix)                                               # get a run work dir
    temp_work_dir = \
      get_temp_dir(use_ephemeral_space=use_ephemeral_space)
    temp_notebook = \
      os.path.join(
        temp_work_dir,
        os.path.basename(temp_work_dir))
    generate_ipynb_from_template(
      template_ipynb_path=notebook_template,
      output_dir=temp_work_dir,
      param_dictionary=input_param_map,
      date_tag=date_tag,
      use_ephemeral_space=use_ephemeral_space)                                  # generate notebook from template
    check_file_path(temp_notebook)                                              # check the notebook for run
    if allow_errors == 0:
      allow_errors=False
    nbconvert_execute_in_singularity(
      image_path=singularity_image_path,
      ipynb_path=temp_notebook,
      input_list=input_list,
      output_path=work_dir,
      output_format=output_format,
      output_file_list=output_file_list,
      timeout=timeout,
      kernel=kernel,
      use_ephemeral_space=use_ephemeral_space,
      allow_errors=allow_errors)
    data_flow_param_dict = {}                                                   # FIXME
    self.param(
      'dataflow_params',
      data_flow_param_dict)
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
      raise

