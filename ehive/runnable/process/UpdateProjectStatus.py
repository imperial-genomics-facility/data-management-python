import os,subprocess
from shlex import quote
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.fileutils import get_temp_dir, remove_dir
from igf_data.utils.fileutils import copy_remote_file
from igf_data.utils.project_status_utils import Project_status

class UpdateProjectStatus(IGFBaseProcess):
  '''
  An ehive runnable class for updating data for project info page
  '''
  def param_defaults(self):
    params_dict=super(UpdateProjectStatus,self).param_defaults()
    params_dict.update({
      'remote_project_path':None,
      'remote_user':None,
      'remote_host':None,
      'status_data_json':'status_data.json',
      'demultiplexing_pipeline_name':None,
      'analysis_pipeline_name':None,
      'sample_igf_id':None,
      'use_ephemeral_space':0,
    })
    return params_dict

  def run(self):
    try:
      project_igf_id = self.param_required('project_igf_id')
      sample_igf_id = self.param_required('sample_igf_id')
      remote_project_path = self.param_required('remote_project_path')
      igf_session_class = self.param_required('igf_session_class')
      remote_user = self.param_required('remote_user')
      remote_host = self.param_required('remote_host')
      status_data_json = self.param('status_data_json')
      demultiplexing_pipeline_name = self.param_required('demultiplexing_pipeline_name')
      analysis_pipeline_name = self.param_required('analysis_pipeline_name')
      use_ephemeral_space = self.param('use_ephemeral_space')

      temp_work_dir = get_temp_dir(use_ephemeral_space=use_ephemeral_space)     # get a temp dir
      ps = \
        Project_status(\
          igf_session_class=igf_session_class,
          project_igf_id=project_igf_id)
      temp_status_output = \
        os.path.join(\
          temp_work_dir,
          status_data_json)                                                     # get path for temp status file
      remote_project_dir = \
        os.path.join(\
          remote_project_path,
          project_igf_id)                                                       # get remote project directory path
      ps.generate_gviz_json_file(\
        output_file=temp_status_output,
        demultiplexing_pipeline=demultiplexing_pipeline_name,
        analysis_pipeline=analysis_pipeline_name)                               # write data to output json file
      remote_file_path = \
        os.path.join(\
          remote_project_dir,
          status_data_json)
      self._check_and_copy_remote_file(\
        remote_user=remote_user,
        remote_host=remote_host,
        source_file=temp_status_output,
        remote_file=remote_file_path)                                           # copy file to remote
      self.param('dataflow_params',
                 {'remote_project_info':'done'})
      remove_dir(temp_work_dir)                                                 # remove temp dir
    except Exception as e:
      message = \
        'project: {2}, sample:{3}, Error in {0}: {1}'.\
          format(\
            self.__class__.__name__,
            e,
            project_igf_id,
            sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise


  @staticmethod
  def _check_and_copy_remote_file(remote_user,remote_host,
                                  source_file,remote_file):
    '''
    An internal static method for copying files to remote path
    
    :param remote_user: Username for the remote server
    :param remote_host: Hostname for the remote server
    :param source_file: Source filepath
    :param remote_file: Remote filepath
    '''
    try:
      if not os.path.exists(source_file):
        raise IOError('Source file {0} not found for copy'.\
                      format(source_file))

      remote_config='{0}@{1}'.format(remote_user,remote_host)
      os.chmod(source_file,
               mode=0o754)
      copy_remote_file(\
        source_path=source_file,
        destinationa_path=remote_file,
        destination_address=remote_config)                                      # create dir and copy file to remote
    except:
      raise