import os
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.fileutils import copy_remote_file, calculate_file_checksum

class TransferAndCheckRemoteBclFile(IGFBaseProcess):
  '''
  A class for transferring files from remote server and checking the file checksum value
  '''
  def param_defaults(self):
    params_dict=super(IGFBaseProcess,self).param_defaults()
    params_dict.update({
            'seqrun_server':'orwell.hh.med.ic.ac.uk',
            'chacksum_type':'md5',
            'seqrun_local_dir':None,
            'seqrun_source':None
           })
    return params_dict


  def run(self):
    try:
      seqrun_igf_id=self.param_required('seqrun_igf_id')
      seqrun_source=self.param_required('seqrun_source')
      seqrun_server=self.param_required('seqrun_server')
      seqrun_local_dir=self.param_required('seqrun_local_dir')
      chacksum_type=self.param_required('checksum_type')
      seqrun_file_name=self.param_required('seqrun_file_name')
      file_md5_value=self.param_required('file_md5')
    
      source_file_path=os.path.join(seqrun_source,
                                    seqrun_igf_id,
                                    seqrun_file_name)                           # get new seqrun path
      dir_name=os.path.dirname(seqrun_file_name)                                # returns dir name or empty strings
      destination_dir=os.path.join(seqrun_local_dir,seqrun_igf_id,dir_name)     # get file copy path 
      if not os.path.exists(destination_dir):
        os.mkdir(destination_dir)                                               # create directory is its not present

      destination_path=os.path.join(seqrun_local_dir,
                                    seqrun_igf_id,
                                    seqrun_file_name)                           # get destination path
      copy_remote_file(source_path=source_file_path,
                       destinationa_path=destination_path, 
                       source_address=seqrun_server)                            # copy remote file
      if not os.path.exists(destination_path):
        raise IOError('failed to copy file {0} for seqrun {1}'.\
                      format(seqrun_file_name,seqrun_igf_id))                   # check destination file after copy

      new_checksum=calculate_file_checksum(destination_path,\
                                           hasher=chacksum_type)                # calculate checksum of the transferred file
      if new_checksum != file_md5_value:
        raise ValueError('checksum not matching for file {0}, expected: {1}, got {2}'.\
                         format(seqrun_file_name,file_md5_value, new_checksum)) # raise error if checksum doesn't match

      self.param('dataflow_params',{'seqrun_file_name':seqrun_file_name})
    except Exception as e:
      message='Error in {0}: {1}'.format(self.__class__.__name__, e)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise