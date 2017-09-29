import os, re, copy, sys
from collections import defaultdict, deque

try:
  if sys.version_info[0] < 3:
    # Python 2.x specific import
    from string import maketrans
except:
  raise

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
    self._reformat_project_and_description()

    # set index column values
    self.index_columns=self._get_index_columns()

  def group_data_by_index_length(self):
    '''
    Function for grouping samplesheet rows based on the combined length of index columns
    Output: A dictionary of samplesheet objects, with combined index length as the key
    By default, this function removes Ns from the index
    '''
    data=self._data
    index_columns=self.index_columns
    data_group=defaultdict(list)

    for row in data:
      index_length=0
      for field in index_columns:
        if field not in list(row.keys()): raise ValueError('field {0} not present in samplesheet {1}'.format(field, self.infile))
        
        index_value=row[field]
        index_value=index_value.replace('N','')
        index_value=index_value.replace('n','')
        row[field]=index_value
        index_length = index_length + len(row[field])
      if index_length:       
        data_group[index_length].append(row)
 
    for index_length in data_group.keys():
      self_tmp=copy.copy(self)
      self_tmp._data=data_group[index_length]
      data_group[index_length]=self_tmp
    return data_group


  def _get_index_columns(self):
    '''
    An internal function for retrieving the index column names
    Output: a list of index column names
    '''
    data_header=self._data_header
    pattern=re.compile('^index', re.IGNORECASE)
    index_columns=[header for header in data_header if re.search(pattern, header)]
    if len(index_columns) < 1:
      raise ValueError('samplesheet {0} doesn\'t have any index column'.format(self.infile))

    # check for possible errors in the index column name
    if len(index_columns) != len(set(index_columns)):
      raise ValueError('samplesheet {0} doesn\'t have unique index column names'.format(self.infile))

    return index_columns


  def get_project_names(self, tag='sample_project'):
    '''
    Function for retrieving unique project names from samplesheet
    Output: A list of unique project name
    Default tag for search: sample_project
    If there are multiple matching headers, the first column will be used
    '''
    data_header=self._data_header
    data=self._data
    pattern=re.compile(tag, re.IGNORECASE)
    project_header_list=list(filter((lambda x: re.search(pattern, x)),data_header))

    if  len(project_header_list)==0:
      raise ValueError('no project information found for samplesheet {0}'.format(self.infile))

    project_header=project_header_list[0]
    project_names=list(set([ row[project_header] for row in data ]))

    if len(project_names)==0:
      raise ValueError('no project name found for samplesheet {0}, column {1}'.format(self.infile, project_header))

    return project_names


  def get_index_count(self):
    '''
    Function for getting index length counts
    Output is a dictionary, with the index columns as the key
    '''
    data=self._data
    index_columns=self.index_columns
    index_count=defaultdict(lambda: defaultdict(int))
 
    for row in data:
      for field in index_columns:
        if field not in list(row.keys()): raise ValueError('field {0} not present in samplesheet {1}'.format(field, self.infile))
        index_len=len(row[field].replace('N','').replace('n',''))
        index_count[field][index_len] += 1
    return index_count


  def get_indexes(self):
    '''
    A method for retrieving the indexes from the samplesheet
    returns a list of indexes
    '''
    data=self._data
    index_columns=self.index_columns
    indexes=list()
    
    for row in data:
      index_val=None
      for field in index_columns:
        if field not in list(row.keys()): 
          raise ValueError('field {0} not present in samplesheet {1}'.format(field, self.infile))
        if index_val is None:
          index_val=row[field]
        else:
          index_val='{0}+{1}'.format(index_val,row[field])
      indexes.append(index_val)
    return indexes


  def _reformat_project_and_description(self, project_field='Sample_Project', description_field='Description' ):
    '''
    A Function for removing the user information from Project field and
    converting ':' to '-' in the description field
    '''
    data=self._data
    
    for row in data:
      if project_field not in list(row.keys()):
        raise ValueError('project field {0} not found in sample sheet {1}'.format(project_field, self.infile))
      
      if description_field not in list(row.keys()):
        raise ValueError('description field {0} not found in sample sheet {1}'.format(description_field, self.infile))

      project=row[project_field].split(':')[0]
      row[project_field]=project
      description=row[description_field]
      row[description_field]=description.replace(':','-')
    self._data=data


  def get_reverse_complement_index(self, index_field='index2'):
    '''
    Function for changing the I5_index present in the index2 field of the 
    samplesheet to intsreverse complement base
    '''
    data=self._data

    for row in data:
      if index_field in list(row.keys()):
        # Only run the reverse complement function if index2 exists
        index=row[index_field]
     
        try:
          if sys.version_info[0] < 3:
            # For Python 2.x, use maketrans
            row[index_field]=index.upper().translate(maketrans('ACGT','TGCA'))[::-1]
          else:
            # For Python 3.x, use str.maketrans
            row[index_field]=index.upper().translate(str.maketrans('ACGT','TGCA'))[::-1]
        except:
          raise

    self._data=data


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


  def check_sample_header(self, section, condition_key):
    '''
    Function for checking SampleSheet header
    Output: zero if its not present or number of occurrence of the term
    '''
    header_data=self._header_data
    if not condition_key or not section:
      raise ValueError('section and condition_key are required for sample header check')
 
    exists=0
    pattern=re.compile('^{}$'.format(condition_key), re.IGNORECASE)
    exists=len([row for row in header_data[section] if re.search(pattern, row.split(',')[0])])
    return exists


  def modify_sample_header(self, section, type, condition_key, condition_value=''):
    '''
    Function for modifying SampleSheet header
    Supported type: 'add' or 'remove'
    condition_value is required for 'add' type
    '''
    header_data=self._header_data
    if ( type.lower().strip() == 'add' ):
      # check if condition key is already present
      if (self.check_sample_header( section=section, condition_key=condition_key)):
         raise ValueError('condition_key {} already present for section {}'.format(condition_key, section))

      # can't use the default condition_value
      if not condition_value:
        raise ValueError('condition_value is required for type {} and key {}'.format(type, condition_key))
      else:
        header_data[section].append('{0},{1}'.format(condition_key,condition_value))
    elif ( type.lower().strip() == 'remove' ):
      filtered_header_section=list()
      pattern=re.compile('^{}$'.format(condition_key), re.IGNORECASE)

      for row in header_data[section]:
        if re.match( pattern, row.split(',')[0] ):
          pass
        else:
          filtered_header_section.append(row)
      header_data[section]=filtered_header_section
    else:
      raise valueError('type {} not supported'.format(type))

    # resetting the header
    self._header_data=header_data


  def filter_sample_data( self, condition_key, condition_value , method='include',lane_header='Lane',lane_default_val='1'):
    '''
    Function for filtering SampleSheet data based on matching condition
    required params:
    condition_key: A samplesheet column name
    condition_value: A keyword present in the selected column
    method: 'include' or 'exclude' for adding or removing selected column from the samplesheet
             default is include
    '''
    condition_value=str(condition_value).strip()
    data_header=self._data_header
    raw_data=self._data
    filtered_data=list()
    
    for row in raw_data:
      if condition_key not in list(row.keys()): 
        if condition_key==lane_header and condition_value==lane_default_val:
          # nextseq and miseq samplesheet doesn't have any lane column, default lane number is 1
          filtered_data.append(row)
        else:
          raise ValueError('key {}, value {} not found for {}'.format(condition_key,type(condition_value),row))
      else:
        if method=='include':
          if row[condition_key] == condition_value: filtered_data.append(row)
        elif method=='exclude':
          if row[condition_key] != condition_value: filtered_data.append(row)
        else:
          raise ValueError('method {0} not supported'.format(method))
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
    if os.path.exists(outfile): 
      raise IOError('output file {} already present'.format(outfile))
    
    with open(outfile, 'w') as file:
      # formatting output
      for header_key in header_data.keys():
        file.write('[{}]\n'.format(header_key))
        header_data_section='\n'.join(header_data[header_key])
        file.write('{}\n'.format(header_data_section))

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
    Output: 2 lists , 1st list of column headers for data section,
            2nd list of dictionaries containing data
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
      row=[row_val.rstrip() for row_val in row]
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