import os,subprocess
from shlex import quote
from shutil import copy2
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.fileutils import copy_remote_file
from igf_data.utils.fileutils import get_temp_dir,remove_dir

class CopyAnalysisFilesToRemote(IGFBaseProcess):
  def param_defaults(self):
    params_dict=super(CopyAnalysisFilesToRemote,self).param_defaults()
    params_dict.update({
      'remote_host':None,
      'remote_user':'igf',
      'remote_project_path':None,
      'remote_seqrun_path':None,
      'force_overwrite':True,
      'dir_labels':[],
      'sample_igf_id':None,
      })
    return params_dict

  def run(self):
    try:
      project_igf_id=self.param_required('project_igf_id')
      sample_igf_id=self.param_required('sample_igf_id')
      file_list=self.param_required('file_list')
      remote_user=self.param_required('remote_user')
      remote_host=self.param_required('remote_host')
      remote_project_path=self.param_required('remote_project_path')
      dir_labels=self.param_required('dir_labels')
      force_overwrite=self.param('force_overwrite')
      destination_output_path=os.path.join(remote_project_path,
                                           project_igf_id)                      # get base destination path
      if isinstance(dir_labels, list) and \
         len(dir_labels) > 0:
        destination_output_path=os.path.join(destination_output_path,
                                             *dir_labels)
      #if sample_igf_id is not None:
      #  destination_output_path=os.path.join(destination_output_path,
      #                                       sample_igf_id)                     # add sample name to the destination path

      output_file_list=list()
      temp_work_dir=get_temp_dir()                                              # get temp dir
      for file in file_list:
        if not os.path.exists(file):
          raise IOError('file {0} not found'.\
                        format(file))

        dest_file_path=os.path.join(destination_output_path,
                                    os.path.basename(file))                     # get destination file path
        #file_check_cmd=['ssh',
        #                '{0}@{1}'.\
        #                format(remote_user,
        #                       remote_host),
        #                'ls',
        #                quote(dest_file_path)]
        #response=subprocess.call(file_check_cmd)                                # check for existing remote file
        #if force_overwrite and response==0:
        #  file_rm_cmd=['ssh',
        #               '{0}@{1}'.\
        #               format(remote_user,
        #                      remote_host),
        #               'rm',
        #               '-f',
        #               quote(dest_file_path)]
        #  subprocess.check_call(file_rm_cmd)                                    # remove remote file if its already present

        copy2(file,os.path.join(temp_work_dir,
                                os.path.basename(file)))                        # copy file to a temp dir
        os.chmod(os.path.join(temp_work_dir,
                              os.path.basename(file)),
                 mode=0o754)                                                    # set file permission
        #remote_mkdir_cmd=['ssh',
        #                  '{0}@{1}'.\
        #                  format(remote_user,
        #                         remote_host),
        #                  'mkdir',
        #                  '-p',
        #                  destination_output_path]
        #subprocess.check_call(remote_mkdir_cmd)                                 # create destination dir
        copy_remote_file(\
          source_path=os.path.join(temp_work_dir,
                                   os.path.basename(file)),
          destinationa_path=dest_file_path,
          destination_address='{0}@{1}'.format(remote_user,remote_host),
          force_update=force_overwrite
        )                                                                       # copy file to remote
        output_file_list.append(dest_file_path)

      remove_dir(dir_path=temp_work_dir)                                        # remove temp dir
      self.param('dataflow_params',
                 {'status': 'done',
                  'output_list':output_file_list})                              # add dataflow params
    except Exception as e:
      message='project: {2}, sample:{3}, Error in {0}: {1}'.\
              format(self.__class__.__name__,
                     e,
                     project_igf_id,
                     sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise