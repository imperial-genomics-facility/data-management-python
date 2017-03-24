import os, re
from collections import defaultdict, deque

class SampleSheet:
  '''
  A class for processing SampleSheet files for Illumina sequencing runs
  '''

  def __init__(self, infile, data_header_name='Data'):
    self.infile=infile 
    self.data_header_name=data_header_name 

    # reading samplesheet data
    self._sample_data=self._read_samplesheet()

    # loading header information
    self._header_data=self._load_header()

    # loading data and data header information
    data_header, raw_data=self._load_data()
    self._data_header=data_header
    self._data=raw_data

  def get_platform_name(self, section='Header', field='Application'):
    '''
    Function for getting platform details from samplesheet header
    Default section is 'Header' and field is 'Application'
    '''

    header_section_data=self._header_data[section]
    pattern=re.compile('^{},'.format(field), re.IGNORECASE)
    match=0
    for row in header_section_data:
      if re.search(pattern, row):
        match=1
        (field_name, machine_name)=row.split(',')[0:2]
        return machine_name
    if match == 0: raise ValueError('samplesheet {0} doesn\'t have the field {1}'.format(self.infile, field))

  def get_lane_count(self, lane_field='Lane', target_platform='HiSeq'):
    '''
    Function for getting the lane information for HiSeq runs
    It will return 1 for both MiSeq and NextSeq runs
    '''

    data_header=self._data_header
    data=self._data
    platform_name=self.get_platform_name()

    lane=set()

    pattern=re.compile('^{}'.format(target_platform), re.IGNORECASE)
 
    if re.search(pattern, platform_name):
      for row in data:
        if lane_field not in list(row.keys()): 
          raise ValueError('lane field {0} not found for platform, {1}, sample sheet {2}'.format(lane_field, target_platform, self.infile))
        lane.add(row[lane_field])
    else:
      lane.add(1)    
    return list(lane)

  def filter_sample_header(self, section, type, condition_key, condition_value=''):
    '''
    Function for filtering SampleSheet header
    Supported type: 'add' or 'remove'
    condition_value is required for 'add' type
    '''
   
    header_data=self._header_data
    if ( type.lower().strip() == 'add' ):
      if not condition_value:
        raise ValueError('condition_value is required for type {} and key {}'.format(type, condition_key))
      else:
        header_data[section].append('{0},{1}'.format(condition_key,condition_value))     
    elif ( type.lower().strip() == 'remove' ):
      filtered_header_section=list()
      pattern=re.compile('^{}'.format(condition_key), re.IGNORECASE)
      for row in header_data[section]:
        if re.match( pattern, row ):
          pass
        else:
          filtered_header_section.append(row)
      header_data[section]=filtered_header_section
    else:
      raise valueError('type {} not supported'.format(type))

    # resetting the header
    self._header_data=header_data 
  
   
  def filter_sample_data( self, condition_key, condition_value ):
    '''
    Function for filtering SampleSheet data based on matching condition
    '''

    condition_value=str(condition_value).strip()
    data_header=self._data_header
    raw_data=self._data
    filtered_data=list()
    
    for row in raw_data:
      if condition_key not in list(row.keys()): 
        raise ValueError('key {} not found for {}'.format(condition_key, row))
      else:
        if row[condition_key] == condition_value: filtered_data.append(row)

    # resetting data information
    self._data=filtered_data

  def print_sampleSheet(self, outfile):
    '''
    Function for printing output SampleSheet
    '''
    header_data=self._header_data
    data_header=self._data_header
    data=self._data
    
    # check if output file exists
    if os.path.exists(outfile): raise IOError('output file {} already present'.format(outfile))

    with open(outfile, 'w') as file:   
      # formatting output
      for header_key in header_data.keys():
        file.write('[{}]\n'.format(header_key))
      for row in header_data[header_key]:
         file.write('{}\n'.format(row))

      file.write('[{}]\n'.format(self.data_header_name))
      file.write('{}\n'.format(','.join(data_header)))

      for row in data:
        data_row=list()
        for h in data_header:
          data_row.append(row[h])
        file.write('{}\n'.format(','.join(data_row)))

  def _load_header(self):
    '''
    Function for loading SampleSheet header
    '''

    sample_data=self._sample_data
    header_data=dict()

    for keys in sample_data:
      if keys != self.data_header_name: 
        header_data[keys]=sample_data[keys]   
    return header_data
  
  def _load_data(self):
    '''
    Function for loading SampleSheet data
    '''

    sample_data=self._sample_data
    data=sample_data[self.data_header_name]
    data=deque(data)
    data_header=data.popleft()
    data_header=data_header.split(',')
    sample_data=list()
    for row in data:
      row=row.split(',')
      row_data=dict(zip(data_header,row))
      sample_data.append(row_data)
    return data_header, sample_data
  
  def _read_samplesheet(self):
    '''
    Function for reading SampleSheet.csv file
    '''

    infile=self.infile

    if os.path.exists(infile) == False:
      raise IOError('file {0} not found'.format(infile))

    sample_data=defaultdict(list)
    header=''

    with open(infile, 'r') as f:
      for i in f:
        row=i.rstrip('\n')
        if row.startswith('['):
           header=row.split(',')[0].strip('[').strip(']')
        else:
           sample_data[header].append(row)
    return sample_data


