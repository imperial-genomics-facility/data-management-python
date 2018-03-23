import shutil,os, subprocess

def get_storage_stats_in_gb(storage_list):
  '''
  A utility function for fetching disk usage stats (df -h) and return disk 
  usge in Gb
  
  :param storage_list, a input list of storage path
  
  returns a list of dictionary containing following keys
     storage
     used
     available
  '''
  try:
    storage_stats=list()
    for storage_path in storage_list:
      if not os.path.exists(storage_path):
        raise IOError('path {0} not found'.format(storage_path))
      else:
        storage_usage=shutil.disk_usage(storage_path)
        storage_stats.append({'storage':storage_path,
                              'used':storage_usage.used/1024/1024/1024,
                              'available':storage_usage.free/1024/1024/1024})
    return storage_stats
  except:
    raise

def get_sub_directory_size_in_gb(input_path):
  '''
  A utility function for listing disk size of all sub-directories for a given path
  (similar to linux command du -sh /path/* )
  
  :param input_path, a input file path
  
  returns a list of dictionaries containing following keys
    directory_name
    directory_size
  '''
  try:
    storage_stats=list()
    for dir_name in os.listdir(input_path):
      cmd=['du','-s',os.path.join(input_path,dir_name)]
      proc=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
      proc.wait()
      outs,errs=proc.communicate()
      if proc.returncode == 0:
        dir_size,dir_name=outs.decode('utf-8').split()
        storage_stats.append({'directory_name':os.path.basename(dir_name),
                              'directory_size':int(dir_size)/1024/1024/1024})
      else:
        raise ValueError('Failed directory size check: {0}'.format(err))
    return storage_stats
  except:
    raise