import os
from collections import defaultdict

def extract_data_from_interop_dump(run_dump):
  """
  A function for extracting data from interop dump
  InterOp dump can be created using the dumptext command

  :param run_dump: A dump csv file
  :returns: A defaultdict object containing all the data
  """
  try:
    if not os.path.exists(run_dump):
      raise IOError('File {0} not found'.format(run_dump))
    data = defaultdict(list)
    header = None
    data_header = None
    with open(run_dump,'r') as fp:
      for line in fp:
        line = line.strip()
        if line.startswith('#'):
          if line.startswith('# Version') or \
             line.startswith('# Column Count') or \
             line.startswith('# Bin Count') or \
             line.startswith('# Channel Count'):
            pass
          else:
            header = line.strip('# ').split(',')[0]
        else:
          if header is not None:
            if 'Lane' in line.split(','):
              data_header = line.split(',')
              continue
            if data_header is not None:
              data[header].\
                append(dict(zip(data_header,line.split(','))))
    return data
  except Exception as e:
    raise ValueError(
            'Failed to extract data from dump {0}, error: {1}'.\
              format(run_dump,e))