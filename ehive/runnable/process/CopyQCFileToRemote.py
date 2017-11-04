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
      tag=self.param_required('tag')
      analysis_label=self.param_required('analysis_label')
      
      if not os.path.exists(file):
        raise IOError('file {0} not found'.format(file))

      lane_info=os.path.basename(os.path.dirname(file))                         # get the lane and index length info
      
      file_name=os.path.basename(file) 
      match=re.match(re.compile(r'(\S+)(\.)(\S?)'),file_name)
      if match:
        file_label=match.group(1)
      else:
        file_label=file_name
      
      destination_outout_path=os.path.join(remote_project_path, \
                                          project_name, \
                                          analysis_label, \
                                          seqrun_date, \
                                          flowcell_id, \
                                          lane_info,\
                                          tag,\
                                          file_label)                           # result dir path is generic
      
      remote_mkdir_cmd=['ssh',\
                        destination_address,\
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