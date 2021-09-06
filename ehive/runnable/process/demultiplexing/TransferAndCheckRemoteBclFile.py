import os
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.fileutils import copy_remote_file, calculate_file_checksum

class TransferAndCheckRemoteBclFile(IGFBaseProcess):
  '''
  A class for transferring files from remote server and checking the file checksum value
  '''
  def param_defaults(self):
    params_dict=super(TransferAndCheckRemoteBclFile,self).param_defaults()
    params_dict.update({
            'seqrun_server':None,
            'chacksum_type':'md5',
            'seqrun_local_dir':None,
            'seqrun_source':None,
            'seqrun_user':None,
            'seqrun_server':None,
           })
    return params_dict


  def run(self):
    try:
      seqrun_igf_id = self.param_required('seqrun_igf_id')
      seqrun_source = self.param_required('seqrun_source')
      seqrun_server = self.param_required('seqrun_server')
      seqrun_user = self.param_required('seqrun_user')
      seqrun_local_dir = self.param_required('seqrun_local_dir')
      chacksum_type = self.param_required('checksum_type')
      seqrun_file_name = self.param_required('seqrun_file_name')
      file_md5_value = self.param_required('file_md5')
      transfer_remote_file = True                                               # transfer file from remote server
      source_file_path = \
        os.path.join(\
          seqrun_source,
          seqrun_igf_id,
          seqrun_file_name)                                                     # get new seqrun path
      dir_name = os.path.dirname(seqrun_file_name)                              # returns dir name or empty strings
      destination_dir = \
        os.path.join(\
          seqrun_local_dir,
          seqrun_igf_id,
          dir_name)                                                             # get file copy path 

      destination_path = \
        os.path.join(\
          destination_dir,
          os.path.basename(seqrun_file_name))                                   # get destination path
      if os.path.exists(destination_path) and \
         os.path.isfile(destination_path):
        existing_checksum = \
          calculate_file_checksum(\
            destination_path,\
            hasher=chacksum_type)                                               # calculate checksum of existing file
        if existing_checksum == file_md5_value:
          transfer_remote_file = False                                          # skip file transfer if its up to date
        else:
          os.remove(destination_path)                                           # remove existing file

      if transfer_remote_file:
        if seqrun_user is None and seqrun_server is None:
          raise ValueError('seqrun: {0}, missing required value for seqrun_user or seqrun_server'.\
                           format(seqrun_igf_id))

        source_address = '{0}@{1}'.format(seqrun_user,seqrun_server)            # get host username and address
        copy_remote_file(\
          source_path=source_file_path,
          destination_path=destination_path, 
          source_address=source_address,
          check_file=False)                                                     # copy remote file
        if not os.path.exists(destination_path):
          raise IOError('failed to copy file {0} for seqrun {1}'.\
                        format(seqrun_file_name,seqrun_igf_id))                 # check destination file after copy

        new_checksum = \
          calculate_file_checksum(\
            destination_path,
            hasher=chacksum_type)                                               # calculate checksum of the transferred file
        if new_checksum != file_md5_value:
          raise ValueError('seqrun:{3}, checksum not matching for file {0}, expected: {1}, got {2}'.\
                           format(seqrun_file_name,
                                  file_md5_value,
                                  new_checksum,
                                  seqrun_igf_id))                               # raise error if checksum doesn't match

      self.param('dataflow_params',{'seqrun_file_name':seqrun_file_name})
    except Exception as e:
      message = \
        'seqrun: {2}, Error in {0}: {1}'.\
          format(\
            self.__class__.__name__,
            e,
            seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      self.post_message_to_ms_team(
          message=message,
          reaction='fail')
      raise