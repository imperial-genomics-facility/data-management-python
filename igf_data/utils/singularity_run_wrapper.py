import os
from spython.main import Client
from igf_data.utils.fileutils import check_file_path,copy_local_file,get_temp_dir,remove_dir

def singularity_run(image_path,path_bind,args_list,return_results=True,use_ephemeral_space=False,dry_run=False):
  '''
  A wrapper module for running singularity based containers

  :param image_path: Singularrity image path
  :param path_bind: Path to bind to singularity /tmp dir
  :param args_list: List of args for singulatiy run
  :param return_results: Return singulatiy run results, default True
  :param use_ephemeral_space: Toggle for using ephemeral space for temp dir, default False
  :param dry_run: Return the singularity command without run, default False
  :returns: A string containing singularity command line
  '''
  try:
    check_file_path(image_path)
    check_file_path(path_bind)
    temp_dir = get_temp_dir(use_ephemeral_space=use_ephemeral_space)
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
    singularity_run_cmd = \
      'singularity run {0} --bind {1}:/tmp {2}'.\
        format(
          temp_image_path,
          path_bind,
          args)
    if dry_run:
      return singularity_run_cmd
    else:
      Client.run(
        image=temp_image_path,
        bind='{0}:/tmp'.format(path_bind),
        args=args,
        return_result=return_results)
      remove_dir(temp_dir)                                                      # remove copied image after run
      return singularity_run_cmd
  except Exception as e:
    raise ValueError(
            'Failed to run image {0}, error: {1}'.\
              format(image_path,e))