import os,subprocess
from shlex import quote
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.fileutils import get_temp_dir, remove_dir
from igf_data.utils.fileutils import copy_remote_file
from igf_data.utils.project_analysis_utils import Project_analysis

class UpdateProjectAnalysisStats(IGFBaseProcess):
  '''
  An ehive runnable class for updating data for project analysis info page
  '''
  def param_defaults(self):
    params_dict=super(UpdateProjectAnalysisStats,self).param_defaults()
    params_dict.update({
      'remote_project_path':None,
      'remote_user':None,
      'remote_host':None,
      'analysis_data_json':'analysis_data.json',
      'sample_igf_id':None,
    })
    return params_dict

  def run(self):
    try:
      project_igf_id=self.param_required('project_igf_id')
      sample_igf_id=self.param_required('sample_igf_id')
      collection_type_list=self.param_required('collection_type_list')
      analysis_data_json=self.param_required('analysis_data_json')
      igf_session_class=self.param_required('igf_session_class')
      remote_project_path=self.param_required('remote_project_path')
      remote_user=self.param_required('remote_user')
      remote_host=self.param_required('remote_host')

      temp_dir=get_temp_dir()
      output_file=os.path.join(temp_dir,analysis_data_json)
      prj_data=Project_analysis(igf_session_class=igf_session_class,
                                collection_type_list=collection_type_list)
      prj_data.\
      get_analysis_data_for_project(project_igf_id=project_igf_id,
                                    output_file=output_file)
      remote_file_path=os.path.join(remote_project_path,
                                    project_igf_id,
                                    analysis_data_json)
      os.chmod(output_file,
               mode=0o754)                                                      # changed file permission before copy
      self._check_and_copy_remote_file(remote_user=remote_user,
                                       remote_host=remote_host,
                                       source_file=output_file,
                                       remote_file=remote_file_path)
      self.param('dataflow_params',{'remote_file_path':remote_file_path})
    except Exception as e:
      message='project: {2}, sample:{3}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
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
      check_remote_cmd=['ssh',
                        quote(remote_config),
                        'ls',
                        '-a',
                        quote(remote_file)]                                     # remote check cmd
      response=subprocess.call(check_remote_cmd)                                # look for existing remote file
      if response !=0:
        rm_remote_cmd=['ssh',
                       quote(remote_config),
                       'rm',
                       '-f',
                       quote(remote_file)]                                      # remote rm cmd
        subprocess.check_call(rm_remote_cmd)                                    # remove existing file

      copy_remote_file(source_path=source_file,
                       destinationa_path=remote_file,
                       destination_address=remote_config)                       # create dir and copy file to remote
    except:
      raise