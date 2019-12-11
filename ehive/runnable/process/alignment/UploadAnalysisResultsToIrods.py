import os
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.igf_irods_client import IGF_irods_uploader
from igf_data.igfdb.projectadaptor import ProjectAdaptor


class UploadAnalysisResultsToIrods(IGFBaseProcess):
  def param_defaults(self):
    params_dict=super(UploadAnalysisResultsToIrods,self).param_defaults()
    params_dict.update({
        'irods_exe_dir':None,
        'analysis_name':'default',
        'dir_path_list':None,
        'file_tag':None
      })
    return params_dict

  def run(self):
    '''
    A ehive runnable method for uploading analysis files to irods server
    
    :param file_list: A list of file paths to upload to irods
    :param irods_exe_dir: Irods executable directory
    :param project_igf_id: Name of the project
    :param analysis_name: A string for analysis name, default is 'default'
    :param dir_path_list: A list of directory structure for irod server, default None for using datestamp
    :param file_tag: A text string for adding tag to collection, default None for only project_name
    '''
    try:
      project_igf_id = self.param_required('project_igf_id')
      igf_session_class = self.param_required('igf_session_class')
      irods_exe_dir = self.param_required('irods_exe_dir')
      file_list = self.param_required('file_list')
      analysis_name = self.param_required('analysis_name')
      dir_path_list = self.param_required('dir_path_list')
      file_tag = self.param_required('file_tag')

      pa = ProjectAdaptor(**{'session_class':igf_session_class})
      pa.start_session()
      user = \
        pa.fetch_data_authority_for_project(
          project_igf_id=project_igf_id)                                        # fetch user info from db
      pa.close_session()

      if user is None:
        raise ValueError('No user found for project {0}'.format(project_igf_id))

      username = user.username                                                  # get username for irods
      irods_upload = IGF_irods_uploader(irods_exe_dir)                          # create instance for irods upload
      for file_path in file_list:
        if not os.path.exists(file_path):
          raise IOError('Failed to find file {0} for irods upload'.\
                        format(file_path))

      irods_upload.\
        upload_analysis_results_and_create_collection(
          file_list=file_list,
          irods_user=username,
          project_name=project_igf_id,
          analysis_name=analysis_name,
          dir_path_list=dir_path_list,
          file_tag=file_tag)                                                    # upload analysis results to irods and build collection
    except Exception as e:
      message = \
        'project: {2}, Error in {0}: {1}'.format(
          self.__class__.__name__,
          e,
          project_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise