import os,subprocess
import pandas as pd
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
      'samplereadcountcsvfile':'samplereadcountfile.csv',
      'status_data_json':'status_data.json',
      'pipeline_name':None,
      'analysis_pipeline_name':None,
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
      samplereadcountcsvfile=self.param('samplereadcountcsvfile')
      status_data_json=self.param('status_data_json')
      pipeline_name=self.param_required('pipeline_name')
      analysis_pipeline_name=self.param_required('analysis_pipeline_name')

      temp_work_dir=get_temp_dir(use_ephemeral_space=True)                      # get a temp dir
      temp_read_count_output=os.path.join(temp_work_dir,
                                          samplereadcountfile)                  # get path for temp read count file
      temp_read_count_csv_output=os.path.join(temp_work_dir,
                                              samplereadcountcsvfile)           # get path for temp read count csv file
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
      read_count_data = pd.DataFrame(read_count_data)
      if not isinstance(read_count_data,pd.DataFrame):
        raise ValueError('Expecting a pandas dataframe, and got {0}'.\
          format(type(read_count_data)))

      read_count_data.to_csv(temp_read_count_csv_output,index=False)            # create csv output for project data
      seqrun_data=get_seqrun_info_for_project(\
        session_class=igf_session_class,
        project_igf_id=project_name)                                            # fetch seqrun info for each projects
      add_seqrun_path_info(input_data=seqrun_data,
                           output_file=temp_seqrun_info)                        # write seqrun info json
      remote_project_dir=os.path.join(remote_project_path,
                                      project_name)                             # get remote project directory path
      os.chmod(temp_seqrun_info,
               mode=0o754)                                                      # changed file permission before copy
      self._check_and_copy_remote_file(\
        remote_user=remote_user,
        remote_host=remote_host,
        source_file=temp_seqrun_info,
        remote_file=os.path.join(remote_project_dir,
                                 seqruninfofile))                               # copy seqrun info file to remote
      os.chmod(temp_read_count_output, mode=0o754)                              # changed file permission before copy
      self._check_and_copy_remote_file(\
        remote_user=remote_user,
        remote_host=remote_host,
        source_file=temp_read_count_output,
        remote_file=os.path.join(remote_project_dir,
                                 samplereadcountfile))                          # copy file sample read count json file to remote
      os.chmod(temp_read_count_csv_output, mode=0o754)                          # changed file permission before copy
      self._check_and_copy_remote_file(\
        remote_user=remote_user,
        remote_host=remote_host,
        source_file=temp_read_count_csv_output,
        remote_file=os.path.join(remote_project_dir,
                                 samplereadcountcsvfile))                       # copy file sample read count csv file to remote

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
      self._check_and_copy_remote_file(\
        remote_user=remote_user,
        remote_host=remote_host,
        source_file=temp_status_output,
        remote_file=os.path.join(remote_project_dir,
                                 status_data_json))                             # copy file project status file to remote
      self.param('dataflow_params',{'remote_project_info':'done'})
      remove_dir(temp_work_dir)                                                 # remove temp dir
    except Exception as e:
      message='seqrun: {2}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      seqrun_igf_id)
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

      check_remote_cmd=['ssh',
                        '{0}@{1}'.\
                        format(remote_user,
                               remote_host),
                        'ls',
                        '-a',
                        remote_file]                                            # remote check cmd
      response=subprocess.call(check_remote_cmd)                                # look for existing remote file
      if response !=0:
        rm_remote_cmd=['ssh',
                       '{0}@{1}'.\
                       format(remote_user,
                              remote_host),
                       'rm',
                       '-f',
                       remote_file]                                             # remote rm cmd
        subprocess.check_call(rm_remote_cmd)                                    # remove existing file

      copy_remote_file(source_path=source_file,
                       destinationa_path=remote_file,
                       destination_address='{0}@{1}'.\
                                           format(remote_user,
                                                  remote_host))                 # create dir and copy file to remote
    except:
      raise