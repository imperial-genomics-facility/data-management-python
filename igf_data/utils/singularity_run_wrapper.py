import os
from spython.main import Client
from igf_data.utils.fileutils import check_file_path,copy_local_file,get_temp_dir

def singularity_run(image_path,path_bind,args_list,return_results=True):
  '''
  A wrapper module for running singularity based containers

  :param image_path: Singularrity image path
  :param path_bind: Path to bind to singularity /tmp dir
  :param args_list: List of args for singulatiy run
  :param return_results: Return singulatiy run results, default True
  '''
  try:
    check_file_path(image_path)
    check_file_path(path_bind)
    temp_dir = get_temp_dir()
    temp_image_path = \
      os.path.join(
        temp_dir,
        os.path.basename(image_path))
    copy_local_file(
      image_path,
      temp_image_path )                                                         # copy image to tmp dir
    if not isinstance(args_list,list) and \
       len(args_list) > 0:
       raise ValueError('No args provided for singularity run')                 # safemode
    args = ' '.join(args_list)                                                  # flatten args
    Client.run(
      image=temp_image_path,
      bind='{0}:/tmp'.format(path_bind),
      args=args,
      return_result=return_results)
  except Exception as e:
    raise ValueError('Failed to run image {0}, error: {1}'.\
                     format(image_path,e))