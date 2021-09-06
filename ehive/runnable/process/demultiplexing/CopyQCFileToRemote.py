import os,datetime,subprocess, re
from shutil import copy2
from igf_data.utils.fileutils import get_temp_dir
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.fileutils import copy_remote_file

class CopyQCFileToRemote(IGFBaseProcess):
  def param_defaults(self):
    params_dict=super(CopyQCFileToRemote,self).param_defaults()
    params_dict.update({
      'remote_host':None,
      'remote_user':'igf',
      'remote_project_path':None,
      'remote_seqrun_path':None,
      'force_overwrite':True,
      'dir_label':None,
      'sample_label':None,
      'use_ephemeral_space':0,
      })
    return params_dict
  
  def run(self):
    try:
      file = self.param_required('file')
      seqrun_igf_id = self.param_required('seqrun_igf_id')
      remote_user = self.param_required('remote_user')
      remote_host = self.param_required('remote_host')
      remote_project_path = self.param_required('remote_project_path')
      project_name = self.param_required('project_name')
      seqrun_date = self.param_required('seqrun_date')
      flowcell_id = self.param_required('flowcell_id')
      dir_label = self.param_required('dir_label')
      sample_label = self.param('sample_label')
      tag = self.param_required('tag')
      use_ephemeral_space = self.param('use_ephemeral_space')
      analysis_label = self.param_required('analysis_label')
      force_overwrite = self.param('force_overwrite')

      if not os.path.exists(file):
        raise IOError('file {0} not found'.format(file))

      if dir_label is None:
        dir_label = \
          os.path.basename(os.path.dirname(file))                               # get the lane and index length info, FIXIT

      file_suffix = None
      file_name = os.path.basename(file)
      file_name_list = file_name.split('.')
      if len(file_name_list) > 1:
        (file_label,file_suffix) = \
          (file_name_list[0],file_name_list[-1])                                # get file_label and suffix
      else:
        file_label = file_name_list[0]

      remote_file_name = \
        '{0}.{1}'.format(analysis_label,file_suffix)                            # simplify remote filename for report page

      destination_outout_path = \
        os.path.join(
          remote_project_path,
          project_name,
          seqrun_date,
          flowcell_id,
          dir_label,
          tag)                                                                  # result dir path is generic
      if sample_label is not None:
        destination_outout_path = \
          os.path.join(
            destination_outout_path,
            sample_label)                                                       # adding sample label only if its present

      destination_outout_path = \
        os.path.join(
          destination_outout_path,
          analysis_label,
          file_label)                                                           # adding file label to the destination path
      if os.path.isfile(file):
        destination_outout_path = \
          os.path.join(\
            destination_outout_path,
            remote_file_name)                                                   # add destination file name

      temp_work_dir = \
        get_temp_dir(use_ephemeral_space=use_ephemeral_space)                   # get a temp work dir
      copy2(
        file,
        os.path.join(
          temp_work_dir,
          remote_file_name))                                                    # copy file to a temp dir and rename it
      os.chmod(
        os.path.join(
          temp_work_dir,
          remote_file_name),
        mode=0o754)                                                             # set file permission
      copy_remote_file(
        source_path=os.path.join(temp_work_dir,remote_file_name),
        destination_path=destination_outout_path,
        destination_address='{0}@{1}'.format(remote_user,remote_host),
        force_update=force_overwrite)                                           # copy file to remote
      if os.path.isdir(file):
        destination_outout_path = \
          os.path.join(
            destination_outout_path,
            remote_file_name)                                                   # add destination dir name

      self.param('dataflow_params',
                 {'file':file,
                  'status': 'done',
                  'remote_file':destination_outout_path})                       # add dataflow params
    except Exception as e:
      message = \
        'seqrun: {2}, Error in {0}: {1}'.\
          format(
            self.__class__.__name__,
            e,
            seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      self.post_message_to_ms_team(
          message=message,
          reaction='fail')
      raise