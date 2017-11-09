import os,datetime,subprocess, re
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.fileutils import copy_remote_file

class CopyQCFileToRemote(IGFBaseProcess):
  def param_defaults(self):
    params_dict=super(IGFBaseProcess,self).param_defaults()
    params_dict.update({
      'remote_host':'eliot.med.ic.ac.uk',
      'remote_project_path':None,
      'remote_seqrun_path':None,
      'force_overwrite':True,
      'dir_label':None,
      'sample_label':None,
      })
    return params_dict
  
  def run(self):
    try:
      file=self.param_required('file')
      
      seqrun_igf_id=self.param_required('seqrun_igf_id')
      remote_user=self.param_required('remote_user')
      remote_host=self.param_required('remote_host')
      remote_project_path=self.param_required('remote_project_path')
      project_name=self.param_required('project_name')
      seqrun_date=self.param_required('seqrun_date')
      flowcell_id=self.param_required('flowcell_id')
      dir_label=self.param_required('dir_label')
      sample_label=self.param('sample_label')
      tag=self.param_required('tag')
      analysis_label=self.param_required('analysis_label')
      force_overwrite=self.param('force_overwrite')
      
      if not os.path.exists(file):
        raise IOError('file {0} not found'.format(file))

      if dir_label is None:
        dir_label=os.path.basename(os.path.dirname(file))                       # get the lane and index length info, FIXIT
      
      file_name=os.path.basename(file)
      
      destination_outout_path=os.path.join(remote_project_path, \
                                          project_name, \
                                          seqrun_date, \
                                          flowcell_id, \
                                          dir_label,\
                                          tag)                                  # result dir path is generic
      if sample_label is not None:
        destination_outout_path=os.path.join(destination_outout_path, \
                                             sample_label)                      # adding sample label only if its present
        
      destination_outout_path=os.path.join(destination_outout_path,\
                                           analysis_label)
      file_check_cmd=['ssh',\
                      '{0}@{1}'.\
                      format(remote_user,\
                             remote_host),\
                      'ls',\
                      os.path.join(destination_outout_path,\
                                   file_name)]
      response=subprocess.call(file_check_cmd)
      if force_overwrite and response==0:
        file_rm_cmd=['ssh',\
                      '{0}@{1}'.\
                      format(remote_user,\
                             remote_host),\
                      'rm', \
                      '-f',\
                      os.path.join(destination_outout_path,\
                                   file_name)]
        subprocess.check_call(file_rm_cmd)                                      # remove remote file if its already present
        
      remote_mkdir_cmd=['ssh',\
                        '{0}@{1}'.\
                        format(remote_user,\
                               remote_host),\
                        'mkdir',\
                        '-p',\
                        destination_outout_path]
      subprocess.check_call(remote_mkdir_cmd)                                   # create destination path
      copy_remote_file(source_path=file, \
                       destinationa_path=destination_outout_path, \
                       destination_address=remote_host)                         # copy file to remote
      self.param('dataflow_params',{'file':file, 'status': 'done'})             # add dataflow params
    except Exception as e:
      message='seqrun: {2}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise