
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.fileutils import get_temp_dir, remove_dir
from igf_data.utils.projectutils import get_project_read_count,get_seqrun_info_for_project
from igf_data.utils.project_data_display_utils import convert_project_data_gviz_data,add_seqrun_path_info

class UpdateProjectInfo(IGFBaseProcess):
  '''
  An ehive runnable class for updating data for project info page
  '''
  def param_defaults(self):
    params_dict=super(CreateRemoteAccessForProject,self).param_defaults()
    params_dict.update({
      'remote_project_path':None,
      'remote_user':None,
      'remote_host':None,
      'seqruninfofile':'seqruninfofile.json',
      'samplereadcountfile':'samplereadcountfile.json',
    })
    return params_dict

  def run(self):
    try:
      seqrun_igf_id=self.param_required('seqrun_igf_id')
      project_name=self.param_required('project_name')
      remote_project_path=self.param_required('remote_project_path')
      igf_session_class=self.param_required('igf_session_class')
      remote_user=self.param_required('remote_user')
      remote_host=self.param_required('remote_host')
      seqruninfofile=self.param('seqruninfofile')
      samplereadcountfile=self.param('samplereadcountfile')

      temp_work_dir=get_temp_dir()                                              # get a temp dir
      temp_read_count_output=os.path.join(temp_work_dir,
                                          samplereadcountfile)                  # get path for temp read count file
      temp_seqrun_info=os.path.join(temp_work_dir,
                                    seqruninfofile)                             # get path for temp seqrun info file
      raw_read_count=get_project_read_count(session_class=igf_session_class,
                                            project_igf_id=project_name)        # get raw read count for project
      convert_project_data_gviz_data(input_data=raw_read_count,
                                     output_file=temp_read_count_output)        # convert read count to gviz json
      seqrun_data=get_seqrun_info_for_project(session_class=igf_session_class,
                                            project_igf_id=project_name)        # fetch seqrun info for each projects
      add_seqrun_path_info(input_data=seqrun_data,
                           output_file=temp_seqrun_info)                        # write seqrun info json
      self.param('dataflow_params',{'remote_project_info':'done'})
      remove_dir(temp_work_dir)                                                 # remove temp dir
    except Exception as e:
      message='seqrun: {2}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise