import os,json
import pandas as pd
from igf_data.utils.fileutils import get_temp_dir,copy_local_file,list_remote_file_or_dirs,check_file_path
from igf_airflow.seqrun.calculate_seqrun_file_size import calculate_seqrun_file_list
from igf_data.process.seqrun_processing.find_and_process_new_seqrun import check_seqrun_dir_in_db
from igf_data.illumina.interop import extract_data_from_interop_dump
from igf_data.illumina.runinfo_xml import RunInfo_xml

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


def compare_existing_seqrun_files(json_path,seqrun_id,seqrun_base_path):
  """
  A function for comparing and re-writing the file list json for ongoing runs

  :param json_path: A json file containing all the seqrun files.
                    This should have the following keys for each list items

                    * file_path
                    * file_size

  :param seqrun_id: Seqrun id
  :param seqrun_base_path: Seqrun base path target dir
  :returns: None
  """
  try:
    check_file_path(json_path)
    hpc_seqrun_path = \
      os.path.join(
        seqrun_base_path,
        seqrun_id)
    if os.path.exists(hpc_seqrun_path):                                         # only compare if traget files are present
      tmp_dir = get_temp_dir()
      tmp_file_list = \
        calculate_seqrun_file_list(
          seqrun_id=seqrun_id,
          seqrun_base_dir=seqrun_base_path,
          output_path=tmp_dir)                                                  # calculate existing files
      transferred_files = \
        pd.read_json(tmp_file_list).\
          set_index('file_path')
      if len(transferred_files.index) > 0:
        all_files = \
          pd.read_json(json_path).\
            set_index('file_path')
        merged_data = \
          all_files.\
            join(
              transferred_files,
              how='left',
              rsuffix='_tmp')                                                   # join both DFs
        merged_data.\
          fillna(0,inplace=True)                                                # set NAN to 0
        merged_data['t'] = \
          pd.np.where(
            merged_data['file_size'] != merged_data['file_size_tmp'],
            'DIFF','')                                                          # tag files as DIFF if existing file is smaller than source
        merged_data['t'] = \
          pd.np.where(
            merged_data.reset_index()['file_path']=='SampleSheet.csv',
            'DIFF',merged_data['t'])                                            # always copy SampleSheet.csv
        target_files = \
          merged_data[merged_data['t']=='DIFF'].\
            reset_index()[['file_path','file_size']]                            # get DF for non existing files
        tmp_json = \
          os.path.join(
            tmp_dir,
            os.path.basename(json_path))
        with open(tmp_json,'w') as jp:
          json.dump(target_files.to_dict(orient='records'),jp)                  # using json lib to preserve file paths
        copy_local_file(
          tmp_json,
          json_path,
          force=True)
  except Exception as e:
    raise ValueError(
            'Failed to compare existing seqrun files for run {0}, error:{1}'.\
              format(seqrun_id,e))


def check_for_sequencing_progress(interop_dump,runinfo_file):
  """
  A function for checking seqrun progress

  :param interop_dump: A dump file created by interop dumptext command
  :param runinfo_file: RunInfo.xml file path for the target sequencing run
  :returns: An int value for current cycle number, a string for index cycle status and
            a list of dictionary for read format
  """
  try:
    data = \
      extract_data_from_interop_dump(run_dump=interop_dump)
    if  'Extraction' not in data.keys():
      raise ValueError(
              'Key Extraction not found in interop dump {0}'.\
                format(interop_dump))    
    extraction = pd.DataFrame(data.get('Extraction'))
    current_cycle = \
      max([int(i) for i in extraction.groupby('Cycle').groups.keys()])
    run_info = RunInfo_xml(runinfo_file)
    read_stat = run_info.get_reads_stats()
    total_cycles = 0
    index_cycles_count = 0
    index_cycles_complete_count = 0
    read_format = list()
    for read_index in sorted(read_stat.keys()):
      isindexedread = \
        read_stat.get(read_index).get('isindexedread')
      numcycles = \
        read_stat.get(read_index).get('numcycles')
      total_cycles += int(numcycles)
      if current_cycle >= total_cycles:
        status =  'Complete'
      else:
        status = 'Incomplete'
      if isindexedread=='Y':
        read_format.\
          append({'I':numcycles})
        index_cycles_count += 1
        if status=='Complete':
          index_cycles_complete_count += 1
      else:
        read_format.\
          append({'R':numcycles})
    index_cycle_status = 'incomplete'
    if index_cycles_complete_count == index_cycles_count:
      index_cycle_status = 'complete'
    return current_cycle,index_cycle_status,read_format
  except Exception as e:
    raise ValueError(
            'Failed to check sequencing progress, error: {0}'.\
              format(e))