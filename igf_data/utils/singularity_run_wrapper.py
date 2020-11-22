import os
from spython.main import Client
from igf_data.utils.fileutils import check_file_path,copy_local_file,get_temp_dir,remove_dir

def singularity_run(image_path,path_bind,args_list,container_dir='/tmp',return_results=True,use_ephemeral_space=False,dry_run=False):
  '''
  A wrapper module for running singularity based containers

  :param image_path: Singularrity image path
  :param path_bind: Path to bind to singularity /tmp dir
  :param args_list: List of args for singulatiy run
  :param return_results: Return singulatiy run results, default True
  :param use_ephemeral_space: Toggle for using ephemeral space for temp dir, default False
  :param dry_run: Return the singularity command without run, default False
  :returns: A response from container run and a string containing singularity command line
  '''
  try:
    check_file_path(image_path)
    check_file_path(path_bind)
    temp_dir = get_temp_dir(use_ephemeral_space=use_ephemeral_space)
    res = None
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
      'singularity run {0} --bind {1}:{2} {3}'.\
        format(
          temp_image_path,
          path_bind,
          container_dir,
          args)
    if dry_run:
      return res,singularity_run_cmd
    else:
      
      res = \
        Client.run(
          image=temp_image_path,
          bind='{0}:{1}'.format(path_bind,container_dir),
          args=args,
          return_result=return_results)
      remove_dir(temp_dir)                                                      # remove copied image after run
      return res,singularity_run_cmd
  except Exception as e:
    raise ValueError(
            'Failed to run image {0}, error: {1}'.\
              format(image_path,e))


def execute_singuarity_cmd(image_path,command_string,log_dir,task_id=1,
                           bind_dir_list=()):
  """
  A function for executing commands within Singularity container

  :param image_path: A Singularity image (.sif) filepath
  :param command_string: A command string to run within container
  :param log_dir: Log dir for dumping errors, if return code is not zero
  :param task_id: Task id for renaming log, default 1
  :param bind_dir_list: List of dirs to bind
  :returns: None
  """
  try:
    check_file_path(image_path)
    check_file_path(log_dir)
    _ = [check_file_path(d)
           for d in bind_dir_list]
    if len(bind_dir_list)==0:
      bind_dir_list = None
    response = \
      Client.execute(
        image=image_path,
        bind=bind_dir_list,
        command=command_string,
        return_result=True)
    return_code = \
      response.get('return_code')
    if return_code != 0 and \
       response.get('return_code') is not None:
      log_file = \
        os.path.join(
          log_dir,
          '{0}.log'.format(task_id))
      with open(log_file,'w') as fp:
        fp.write(response.get('message'))
      raise ValueError(
              'Failed to run command for task id: {0}, log dir: {1}'.\
                format(task_id,log_file))
  except Exception as e:
    raise ValueError('Failed to execute singularity cmd, error: {0}'.format(e))