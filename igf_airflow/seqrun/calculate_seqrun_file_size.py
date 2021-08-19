import os,json
from igf_data.utils.fileutils import get_temp_dir,check_file_path,copy_local_file

def calculate_seqrun_file_list(
      seqrun_id,seqrun_base_dir,output_path,
      exclude_dirs=('Config','Logs','RTALogs','Thumbnail_Images',
                    'Recipe','PeriodicSaveRates')):
  """
  A function for seqrun filesize calculation

  :param seqrun_id: Seqrun id
  :param seqrun_base_dir: Seqrun base dir
  :param output_path: Path to write output json
  :param exclude_dirs: List of dirs to exclude, default are the following

    * Config
    * Logs
    * RTALogs
    * Thumbnail_Images
    * Recipe
    * PeriodicSaveRates

  :returns: A string of output path
  """
  try:
    seqrun_path = os.path.join(seqrun_base_dir,seqrun_id)
    output_filename = '{0}_filesize.json'.format(seqrun_id)
    tmp_dir = get_temp_dir()
    output_json = os.path.join(output_path,output_filename)
    tmp_json = os.path.join(tmp_dir,output_filename)
    check_file_path(seqrun_path)
    check_file_path(output_path)
    file_list = list()
    for root,dirs,files in os.walk(seqrun_path):
      dirs[:] = [d for d in dirs if d not in exclude_dirs]
      for f in files:
        file_path = os.path.join(seqrun_path,root,f)
        file_size = os.path.getsize(file_path)
        file_rel_path = os.path.relpath(file_path,seqrun_path)
        file_list.\
          append({
            'file_path':file_rel_path,
            'file_size':file_size})
    with open(tmp_json,'w') as jp:
      json.dump(file_list,jp)
    copy_local_file(
      tmp_json,
      output_json,
      force=True)
    return output_json
  except Exception as e:
    raise ValueError(
            'Failed to prepare file list for seqrun {0}, error: {1}'.\
              format(seqrun_id,e))