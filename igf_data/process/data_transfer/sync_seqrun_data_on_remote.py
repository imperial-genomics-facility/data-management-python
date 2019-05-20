import os
from igf_data.utils.fileutils import list_remote_file_or_dirs,copy_remote_file,check_file_path()
from igf_data.process.seqrun_processing.find_and_process_new_seqrun import check_seqrun_dir_in_db

class Sync_seqrun_data_from_remote:
  '''
  A python class for syncing sequencing runs from remote servers

  :param seqrun_server: Remote server address
  :param seqrun_path: Base dir path in remote server
  :param database_config_file:Database config file
  :param output_dir: Local path for file copy
  '''
  def __init__(self,seqrun_server,seqrun_path,database_config_file,output_dir):
    self.seqrun_server = seqrun_server
    self.seqrun_path = seqrun_path
    self.database_config_file = database_config_file
    self.output_dir = output_dir

  def run_sync(self):
    '''
    A method for running the sequencing run sync
    '''
    try:
      check_file_path(self.output_dir)
      all_seqrun_dir = \
        list_remote_file_or_dirs(\
          remote_server=self.seqrun_server,
          remote_path=self.seqrun_path,
          only_dirs=True)
      all_seqrun_dir = \
        list(map(os.path.basename,all_seqrun_dir))                              # convert paths to dirname
      new_seqrun_dirs = \
        check_seqrun_dir_in_db(\
          all_seqrun_dir=all_seqrun_dir,
          dbconfig=self.database_config_file)                                   # filter existing seqruns
      for seqrun in new_seqrun_dirs:
        try:
          new_seqruns = \
            check_seqrun_dir_in_db(\
              all_seqrun_dir=[seqrun],
              dbconfig=self.database_config_file)                               # filter existing seqrun again
          if len(new_seqruns)>0:
            copy_remote_file(\
              source_path=os.path.join(self.seqrun_path,seqrun),
              destinationa_path=self.output_dir,
              source_address=self.seqrun_server)                                # sync dirs if its still new

        except Exception as e:
          raise ValueError('Failed to sync seqrun {0}, got error {1}'.\
                           format(seqrun,e))

    except Exception as e:
      raise ValueError('Stopped syncing seqrun data, got error: {0}'.\
                       format(e))
