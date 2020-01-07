import os
from shutil import copytree
from igf_data.utils.singularity_run_wrapper import singularity_run
from igf_data.utils.fileutils import get_temp_dir,remove_dir,check_file_path,copy_local_file

def nbconvert_execute_in_singularity(image_path,ipynb_path,input_list,output_path,output_format='html',
                                     output_file_list=None,timeout=600,kernel='python3',
                                     use_ephemeral_space=False,allow_errors=False):
  '''
  A function for running jupyter nbconvert within singularity containers

  :param image_path: A singularity image path
  :param ipynb_path: A notebook file path to run in the singularity container
  :param input_list: A list of input file for notebook run
  :param output_path: Path to copy output files
  :param output_format: Notebook output format, default html
  :param output_file_list: A list of output files to copy to output_path from tmp dir, default None
  :param timeout: Timeout setting for notebook execution, default 600s
  :param kernel: Kernel name for notebook execution, default python3
  :param allow_errors: A toggle for running notebook with errors, default False
  :param use_ephemeral_space: Toggle for using ephemeral space for temp dir, default False
  '''
  try:
    check_file_path(image_path)
    check_file_path(ipynb_path)
    if not isinstance(input_list,list) and \
       len(input_list)==0:
       raise ValueError("Missing input files for notebook run")
    tmp_dir = get_temp_dir(use_ephemeral_space=use_ephemeral_space)             # this will be mounted on container on /tmp
    tmp_input_list = list()
    for f in input_list:
      check_file_path(f)
      temp_path = \
        os.path.join(
          tmp_dir,
          os.path.basename(f))
      copy_local_file(f,temp_path)                                              # copy input files to temp dir
      tmp_input_list.append(temp_path)
    temp_ipynb_path = \
      os.path.join(
        tmp_dir,
        os.path.basename(ipynb_path))
    copy_local_file(
      ipynb_path,
      temp_ipynb_path)                                                          # copy ipynb file to tmp dir
    args_list = [
      'jupyter',
      'nbconvert',
      '-to={0}'.format(output_format),
      '--execute',
      '--ExecutePreprocessor.enabled=True',
      '--ExecutePreprocessor.timeout={0}'.format(timeout),
      '--ExecutePreprocessor.kernel_name={0}'.format(kernel),
      '/tmp/{0}'.format(os.path.basename(temp_path))]
    if allow_errors:
      args_list.append('--allow-errors')                                        # run notebooks with errors
    singularity_run(
      image_path=image_path,
      path_bind=tmp_dir,
      args_list=args_list)
    try:
      for output in output_file_list:
        temp_output = \
          os.path.join(
            tmp_dir,
            output)
        check_file_path(temp_output)
        if os.path.isfile(temp_output):
          final_output = \
            os.path.join(
              output_path,
              output)
          copy_local_file(
            temp_output,
            final_output)
        elif os.path.isdir(temp_output):
          copytree(
            temp_output,
            output_path)
    except Exception as e:
      raise ValueError(
              "Failed to copy file {0}, error: {1}".\
                format(output,e))
    remove_dir(tmp_dir)
  except Exception as e:
    raise ValueError(
            "Failed to run nbconvert in singularity, error: {0}".\
              format(e))