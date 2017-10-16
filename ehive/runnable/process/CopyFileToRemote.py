import os,datetime,subprocess
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.fileutils import copy_remote_file

class CopyQCFileToRemote(IGFBaseProcess):
  def param_defaults(self):
    params_dict=IGFBaseProcess.param_defaults()
    params_dict.update({
      'remote_host':None,
      'remote_project_path':None,
      'remote_seqrun_path':None,
      'project_igf_id':None,
      'sample_igf_id':None,
      'run_igf_id':None,
      'tag':None,
      'lane_id':None
      })
    return params_dict
  
  def run(self):
    try:
      file=self.param_required('file')
      remote_host=self.param_required('remote_host')
      remote_project_path=self.param_required('remote_project_path')
      remote_seqrun_path=self.param_required('remote_seqrun_path')
      project_igf_id=self.param_required('project_igf_id')
      sample_igf_id=self.param_required('sample_igf_id')
      seqrun_igf_id=self.param_required('seqrun_igf_id')
      run_igf_id=self.param_required('run_igf_id')
      tag=self.param_required('tag')
      lane_id=self.param_required('tag')
      
      if not os.path.exists(file):
        raise IOError('file {0} not found'.format(file))
      
      seqrun_date=seqrun_igf_id.split('_')[0]                                   # collect seqrun date from igf id
      seqrun_date=datetime.datetime.strptime(seqrun_date,'%y%m%d').date()       # identify actual date
      
      if tag.loqwe()=='known':
        destination_outout_path=os.path.join(remote_project_path, \
                                             project_igf_id, \
                                             seqrun_date, \
                                             sample_igf_id, \
                                             run_igf_id )                       # get destination path for known barcodes
      elif tag.lower()=='undetermined':
        if lane_id is None:
          raise ValueError('lane_id is required for copying undetermined files')
        
        destination_outout_path=os.path.join(remote_project_path, \
                                             remote_seqrun_path, \
                                             seqrun_date, \
                                             seqrun_igf_id, \
                                             lane_id
                                            )                                   # get destination path for undetermined barcodes
      else:
        raise ValueError('tag {0} is unknown'.format(tag))
      
      subprocess.check_call(['mkdir','-p',destination_outout_path])             # create destination path
      copy_remote_file(source_path=file, \
                       destinationa_path=destination_outout_path, \
                       destination_address=remote_host)                         # copy file to remote
      self.param('dataflow_params',{'file':file, 'status': 'done'})             # add dataflow params
    except:
      raise 