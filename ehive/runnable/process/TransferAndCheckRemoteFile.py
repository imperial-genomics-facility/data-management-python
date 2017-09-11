import os
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.fileutils import copy_remote_file

class TransferAndCheckRemoteFile(IGFBaseProcess):
  '''
  A class for transferring files from remote server and checking the file checksum value
  '''
  def param_defaults(self):
    return {'log_slack':True,
            'log_asana':True,
            'seqrun_server':'orwell.hh.med.ic.ac.uk',
            'chacksum_type':'md5',
            'seqrun_local_dir':None,
            'seqrun_source':None
           }
    
    
  def run(self):
    sequn_igf_id=self.param_required('seqrun_igf_id')
    seqrun_source=self.param_required('seqrun_source')
    seqrun_server=self.param_required('seqrun_server')
    seqrun_local_dir=self.seqrun_local_dir('seqrun_local_dir')
    chacksum_type=self.param_required('checksum_type')
    seqrun_file_name=self.param_required('seqrun_file_name')
    file_md5_value=self.param_required('file_md5')