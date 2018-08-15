import os,subprocess
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.fileutils import get_temp_dir, remove_dir
from igf_data.utils.fileutils import copy_remote_file
from igf_data.utils.projectutils import get_project_read_count,get_seqrun_info_for_project
from igf_data.utils.gviz_utils import convert_to_gviz_json_for_display
from igf_data.utils.project_data_display_utils import convert_project_data_gviz_data,add_seqrun_path_info
from igf_data.utils.project_status_utils import Project_status

class UpdateProjectInfo(IGFBaseProcess):
  '''
  An ehive runnable class for updating data for project info page
  '''
  def param_defaults(self):
    params_dict=super(UpdateProjectInfo,self).param_defaults()
    params_dict.update({
      'remote_project_path':None,
      'remote_user':None,
      'remote_host':None,
      'seqruninfofile':'seqruninfofile.json',
      'samplereadcountfile':'samplereadcountfile.json',
      'status_data_json':'status_data.json',
      'pipeline_name':None,
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
      status_data_json=self.param('status_data_json')
      pipeline_name=self.param_required('pipeline_name')
      analysis_pipeline_name=self.param_required('analysis_pipeline_name')

      temp_work_dir=get_temp_dir()                                              # get a temp dir
      temp_read_count_output=os.path.join(temp_work_dir,
                                          samplereadcountfile)                  # get path for temp read count file
      temp_seqrun_info=os.path.join(temp_work_dir,
                                    seqruninfofile)                             # get path for temp seqrun info file
      raw_read_count=get_project_read_count(session_class=igf_session_class,
                                            project_igf_id=project_name)        # get raw read count for project
      (description,read_count_data,column_order)=\
              convert_project_data_gviz_data(input_data=raw_read_count)         # convert read count to gviz requirements
      convert_to_gviz_json_for_display(description=description,
                                       data=read_count_data,
                                       columns_order=column_order,
                                       output_file=temp_read_count_output)      # write data to output json file
      seqrun_data=get_seqrun_info_for_project(session_class=igf_session_class,
                                            project_igf_id=project_name)        # fetch seqrun info for each projects
      add_seqrun_path_info(input_data=seqrun_data,
                           output_file=temp_seqrun_info)                        # write seqrun info json
      remote_project_dir=os.path.join(remote_project_path,\
                                      project_name)                             # get remote project directory path
      check_seqrun_cmd=['ssh',\
                        '{0}@{1}'.\
                        format(remote_user,\
                               remote_host),\
                        'ls',\
                        os.path.join(remote_project_dir,seqruninfofile)]
      response=subprocess.call(check_seqrun_cmd)                                # check for existing seqruninfofile
      if response !=0:
        rm_seqrun_cmd=['ssh',\
                       '{0}@{1}'.\
                       format(remote_user,\
                              remote_host),\
                       'rm',\
                       '-f',\
                       os.path.join(remote_project_dir,
                                    seqruninfofile)]
        subprocess.check_call(rm_seqrun_cmd)                                    # remove existing seqruninfofile
      os.chmod(temp_seqrun_info, mode=0o754)                                    # changed file permission before copy
      copy_remote_file(source_path=temp_seqrun_info, \
                       destinationa_path=remote_project_dir, \
                       destination_address=remote_host)                         # copy file to remote
      check_readcount_cmd=['ssh',\
                           '{0}@{1}'.\
                           format(remote_user,\
                                  remote_host),\
                           'ls',\
                           os.path.join(remote_project_dir,
                                        samplereadcountfile)]                   # check for existing samplereadcountfile
      response=subprocess.call(check_readcount_cmd)
      if response !=0:
        rm_readcount_cmd=['ssh',\
                          '{0}@{1}'.\
                          format(remote_user,\
                                 remote_host),\
                          'rm',\
                          '-f',\
                          os.path.join(remote_project_dir,
                                       samplereadcountfile)]
        subprocess.check_call(rm_readcount_cmd)                                 # remove existing samplereadcountfile
      os.chmod(temp_read_count_output, mode=0o754)                              # changed file permission before copy
      copy_remote_file(source_path=temp_read_count_output, \
                       destinationa_path=remote_project_dir, \
                       destination_address=remote_host)                         # copy file to remote

      ps=Project_status(igf_session_class=igf_session_class,
                        project_igf_id=project_name)
      temp_status_output=os.path.join(temp_work_dir,
                                      status_data_json)                         # get path for temp status file
      ps.generate_gviz_json_file(\
           output_file=temp_status_output,
           demultiplexing_pipeline=pipeline_name,
           analysis_pipeline=analysis_pipeline_name,
           active_seqrun_igf_id=seqrun_igf_id)                                  # write data to output json file
      os.chmod(temp_status_output,
               mode=0o754)                                                      # changed file permission before copy
      check_status_cmd=['ssh',\
                        '{0}@{1}'.\
                        format(remote_user,\
                               remote_host),\
                        'ls',\
                        os.path.join(remote_project_dir,
                                     status_data_json)]
      response=subprocess.call(check_status_cmd)                                # check for existing status data file
      if response !=0:
        rm_status_cmd=['ssh',\
                       '{0}@{1}'.\
                       format(remote_user,\
                              remote_host),\
                       'rm',\
                       '-f',\
                       os.path.join(remote_project_dir,
                                    status_data_json)]
        subprocess.check_call(rm_status_cmd)                                    # remove existing status file

      copy_remote_file(source_path=temp_status_output, \
                       destinationa_path=remote_project_dir, \
                       destination_address=remote_host)                         # copy file to remote
      self.param('dataflow_params',{'remote_project_info':'done'})
      remove_dir(temp_work_dir)                                                 # remove temp dir
    except Exception as e:
      message='seqrun: {2}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise