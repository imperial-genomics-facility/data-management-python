import os
from igf_data.utils.fileutils import list_remote_file_or_dirs
from igf_data.process.seqrun_processing.find_and_process_new_seqrun import check_seqrun_dir_in_db

def fetch_all_seqruns(seqrun_server,seqrun_base_path,user_name=None):
  """
  A function for fetching all the seqrun dir names

  :param seqrun_server: Server hostname
  :param seqrun_base_path: Seqrun dir path on host dir
  :returns: A list of seqrun ids
  """
  try:
    all_seqrun_ids = \
      list_remote_file_or_dirs(
        remote_server=seqrun_server,
        remote_path=seqrun_base_path,
        user_name=user_name,
        only_dirs=True)
    all_seqrun_ids = \
      [os.path.basename(i)
        for i in all_seqrun_ids]
    return all_seqrun_ids
  except Exception as e:
    raise ValueError(
            'Failed to fetch all the runs, error: {0}'.format(e))


def check_for_finished_seqrun(
      seqrun_id_list,seqrun_server,seqrun_base_path,
      user_name=None,required_file='RTAComplete.txt'):
  """
  A function for checking ongoing seqruns

  :param seqrun_id_list: A list of seqrun ids
  :param seqrun_server: Seqrun server host name
  :param seqrun_base_path: Seqrun dir base path
  :param required_file: File to check if seqrun in finished, default RTAComplete.txt
  :returns: A list of finished and another list for ongoing run ids
  """
  try:
    if not isinstance(seqrun_id_list,list):
      raise TypeError(
              'Expecting a list of seqrun ids and got {0}'.\
                format(type(seqrun_id_list)))
    ongoing_seqrun_list = list()
    finished_seqrun_list = list()
    for seqrun_id in seqrun_id_list:
      seqrun_path = \
        os.path.join(seqrun_base_path,seqrun_id)
      seqrun_files = \
        list_remote_file_or_dirs(
          remote_server=seqrun_server,
          remote_path=seqrun_path,
          user_name=user_name,
          only_dirs=False,
          only_files=True)                                                      # fetch all the files
      seqrun_file_names = \
        [os.path.basename(i)
          for i in seqrun_files
            if i != seqrun_path]
      if required_file in seqrun_file_names:
        finished_seqrun_list.\
          append(os.path.basename(seqrun_id))
      else:
        ongoing_seqrun_list.\
          append(os.path.basename(seqrun_id))
    return finished_seqrun_list,ongoing_seqrun_list
  except Exception as e:
    raise ValueError(
            'Failed to check for finished sequencing, error: {0}'.\
              format(e))


def fetch_ongoing_seqruns(
      seqrun_server,seqrun_base_path,database_config_file,
      user_name=None,required_file='RTAComplete.txt'):
  """
  A function for fetching a list of ongoing seqrun ids

  :param seqrun_server: Seqrun server hostname
  :param seqrun_base_path: Seqrun dir base path
  :param database_config_file: DB config file path
  :param required_file: Required file to check finished seqrun, default 'RTAComplete.txt'
  :returns: A list of ongoing seqrun ids
  """
  try:
    all_seqrun_ids = \
      fetch_all_seqruns(
        seqrun_server=seqrun_server,
        seqrun_base_path=seqrun_base_path,
        user_name=user_name)
    new_seqrun_ids = \
      check_seqrun_dir_in_db(
        all_seqrun_dir=all_seqrun_ids,
        dbconfig=database_config_file)
    _,ongoing_seqrun_list = \
      check_for_finished_seqrun(
        seqrun_id_list=new_seqrun_ids,
        seqrun_server=seqrun_server,
        seqrun_base_path=seqrun_base_path,
        required_file=required_file,
        user_name=user_name)
    return ongoing_seqrun_list
  except Exception as e:
    raise ValueError(
            'Failed to fetch ongoing seqrun ids, error: {0}'.\
              format(e))