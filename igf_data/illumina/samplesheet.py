import os, re, copy, sys,json
import pandas as pd
import numpy as np
from jsonschema import Draft4Validator
from collections import defaultdict, deque

class SampleSheet:
  '''
  A class for processing SampleSheet files for Illumina sequencing runs

  :param infile: A samplesheet file
  :param data_header_name: name of the data section, default Data
  '''

  def __init__(self, infile, data_header_name=('Data', 'BCLConvert_Data')):
    self.infile = infile
    self.data_header_name = data_header_name
    self._sample_data = self._read_samplesheet()                                # reading samplesheet data
    self._header_data = self._load_header()                                     # loading header information
    data_header, raw_data = self._load_data()                                   # loading data and data header information
    self._data_header = data_header
    self._data = raw_data
    self._reformat_project_and_description()
    self.index_columns = self._get_index_columns()                              # set index column values
    self.columns = ("Lane", "Sample_ID", "Sample_Name", "Sample_Plate",
                    "Sample_Well", "I7_Index_ID", "index", "I5_Index_ID",
                    "index2", "Sample_Project", "Description")

  @property
  def samplesheet_version(self):
    return self._samplesheet_version


  @staticmethod
  def _check_samplesheet_data_row(data_series, single_cell_flag='10X'):
    '''
    An internal static method for additional validation of samplesheet data

    :param data_series: A pandas data series, containing a samplesheet data row
    :param single_cell_flag: A keyword for single cell sample description, default 10X
    :returns: A string of error messages, or NAN value
    '''
    try:
      if not isinstance(data_series, pd.Series):
        raise AttributeError(type(data_series))
      single_cell_flag_pattern = \
        re.compile(
          r'^{0}$'.format(single_cell_flag),
          re.IGNORECASE)
      err = list()
      if ('Sample_ID' in data_series and 'Sample_Name' in data_series) and \
         data_series['Sample_ID']==data_series['Sample_Name']:
        err.append("Same sample id and sample names are not allowed, {0}".\
                   format(data_series['Sample_ID']))
      if ('I5_Index_ID' in data_series and data_series['I5_Index_ID'] != '') and \
         ('index2' not in data_series or data_series['index2'] == ''):
        err.append("Missing I_5 index sequences for {0}".\
                   format(data_series['Sample_ID']))
      single_cell_index_pattern = \
        re.compile(r'^SI-[GNT][ATN]-[A-Z][0-9]+')
      if re.search(single_cell_flag_pattern, data_series['Description']) and \
         not re.search(single_cell_index_pattern, data_series['index']):
        err.append("Required I_7 single cell indexes for 10X sample {0}".\
                   format(data_series['Sample_ID']))
      if not re.search(single_cell_flag_pattern, data_series['Description']) and \
         re.search(single_cell_index_pattern, data_series['index']):
        err.append("Found I_7 single cell indexes, missing 10X description sample {0}".\
                   format(data_series['Sample_ID']))
      if re.search(single_cell_flag_pattern, data_series['Description']) and \
         re.search(single_cell_index_pattern, data_series['index']) and \
         'index2' in data_series and data_series['index2'] !='':
        err.append("Found I_5 index(2) for single cell sample {0}".\
                   format(data_series['Sample_ID']))
      if len(err) == 0:
        err_str = np.nan
      else:
        err_str = '\n'.join(err)
      return err_str
    except:
      raise


  def validate_samplesheet_data(self, schema_json):
    '''
    A method for validation of samplesheet data

    :param schema: A JSON schema for validation of the samplesheet data
    :return a list of error messages or an empty list if no error found
    '''
    try:
      data = self._data
      data = pd.DataFrame(data)                                                 # read data as pandas dataframe
      data = data.fillna("").applymap(lambda x: str(x))                         # replace nan with empty strings and convert all entries to string
      json_data = data.to_dict(orient='records')                                # convert dataframe to list of dictionaries
      error_list = list()                                                       # define empty error list
      if not os.path.exists(schema_json):
        raise IOError('json schema file {0} not found'.format(schema_json))
      with open(schema_json,'r') as jf:
        schema = json.load(jf)                                                  # read schema from the json file
      # syntactic validation
      v_s = Draft4Validator(schema)                                             # initiate validator using schema
      error_list = \
        sorted(v_s.iter_errors(json_data), key=lambda e: e.path)                # overwrite error_list with validation error
      # semantic validation
      other_errors = \
        data.apply(
          lambda x: \
            self._check_samplesheet_data_row(data_series=x),
          axis=1)                                                               # check for additional errors
      other_errors.dropna(inplace=True)
      if len(other_errors) > 0:
        error_list.extend(
          [value for value in other_errors.to_dict().values()])                 # add other errors to the list
      for c in data.columns.tolist():
        if c not in self.columns:
          error_list.\
            append('Unknown column {0} found on samplesheet'.\
                     format(c))
      return error_list
    except:
      raise


  def group_data_by_index_length(self):
    '''
    Function for grouping samplesheet rows based on the combined length of index columns
    By default, this function removes Ns from the index

    :returns: A dictionary of samplesheet objects, with combined index length as the key
    '''
    try:
      data = self._data
      index_columns = self.index_columns
      data_group = defaultdict(list)
      for row in data:
        index_length = 0
        for field in index_columns:
          if field not in list(row.keys()):
            raise ValueError(
                    'field {0} not present in samplesheet {1}'.\
                      format(field, self.infile))
          index_value = row[field]
          index_value = index_value.replace('N', '')
          index_value = index_value.replace('n', '')
          row[field] = index_value
          index_length = index_length + len(row[field])
        if index_length:
          data_group[index_length].append(row)
      for index_length in data_group.keys():
        self_tmp = copy.copy(self)
        self_tmp._data = data_group[index_length]
        data_group[index_length] = self_tmp
      return data_group
    except:
      raise


  def _get_index_columns(self):
    '''
    An internal function for retrieving the index column names

    :returns: A list of index column names
    '''
    try:
      data_header = self._data_header
      pattern = re.compile('^index', re.IGNORECASE)
      index_columns = [
        header for header in data_header \
          if re.search(pattern, header)]
      if len(index_columns) < 1:
        raise ValueError(
                'samplesheet {0} doesn\'t have any index column'.\
                  format(self.infile))
      # check for possible errors in the index column name
      if len(index_columns) != len(set(index_columns)):
        raise ValueError(
                'samplesheet {0} doesn\'t have unique index column names'.\
                  format(self.infile))
      return index_columns
    except:
      raise


  def get_project_names(self, tag='sample_project'):
    '''
    Function for retrieving unique project names from samplesheet.
    If there are multiple matching headers, the first column will be used

    :param tag: Name of tag for project lookup, default sample_project
    :returns: A list of unique project name
    '''
    try:
      data_header = self._data_header
      data = self._data
      pattern = re.compile(tag, re.IGNORECASE)
      project_header_list = \
        list(filter((lambda x: re.search(pattern, x)), data_header))
      if len(project_header_list) == 0:
        raise ValueError(
                'no project information found for samplesheet {0}'.\
                  format(self.infile))
      project_header = project_header_list[0]
      project_names = \
        list(set([row[project_header] for row in data ]))
      if len(project_names) == 0:
        raise ValueError(
                'no project name found for samplesheet {0}, column {1}'.\
                  format(self.infile, project_header))
      return project_names
    except:
      raise


  def get_project_and_lane(self, project_tag='Sample_Project', lane_tag='Lane'):
    '''
    A method for fetching project and lane information from samplesheet

    :param project_tag: A string for project name column in the samplesheet, default Sample_Project
    :param lane_tag: A string for Lane id column in the samplesheet, default Lane
    :returns: A list of project name (for all) and lane information (only for hiseq)
    '''
    try:
      samplesheet_data = pd.DataFrame(self._data)
      samplesheet_columns = list(samplesheet_data.columns)
      if lane_tag in samplesheet_columns:
        data_group = \
          samplesheet_data.groupby([project_tag, lane_tag])                   # for hiseq
      else:
        data_group = \
          samplesheet_data.groupby([project_tag])                          # for nextseq and miseq
      project_list = list()
      for project_lane, _ in data_group:
        if isinstance(project_lane, tuple):
          project_lane = ' : '.join(project_lane)                               # for hiseq samplesheet
        project_list.append(project_lane)
      return project_list
    except:
        raise


  def get_index_count(self):
    '''
    A function for getting index length counts

    :returns: A dictionary, with the index columns as the key
    '''
    try:
      data = self._data
      index_columns = self.index_columns
      index_count = defaultdict(lambda: defaultdict(int))
      for row in data:
        for field in index_columns:
          if field not in list(row.keys()):
            raise ValueError(
                    'field {0} not present in samplesheet {1}'.\
                      format(field, self.infile))
          index_len = \
            len(row[field].replace('N','').replace('n',''))
          index_count[field][index_len] += 1
      return index_count
    except:
      raise


  def get_indexes(self):
    '''
    A method for retrieving the indexes from the samplesheet

    :returns: A list of index barcodes
    '''
    try:
      data = self._data
      index_columns = self.index_columns
      indexes = list()
      for row in data:
        index_val = None
        for field in index_columns:
          if field not in list(row.keys()): 
            raise ValueError(
                    'field {0} not present in samplesheet {1}'.\
                      format(field, self.infile))
          index_seq = row[field]
          index_seq = index_seq.strip().strip('\n')
          if index_seq and index_seq is not None:
            if index_val is None:
              index_val = index_seq
            else:
              index_val = '{0}+{1}'.format(index_val,index_seq)
        indexes.append(index_val)
      return indexes
    except:
      raise


  def add_pseudo_lane_for_miseq(self, lane='1'):
    '''
    A method for adding pseudo lane information for the nextseq platform

    :param lane: A lane id for pseudo lane value
    '''
    try:
      data = self._data
      newdata = list()
      for row in data:
        temp_row = copy.deepcopy(row)
        temp_row['PseudoLane'] = lane
        newdata.append(temp_row)
      self._data = newdata
    except:
      raise


  def add_pseudo_lane_for_nextseq(self, lanes=('1','2','3','4')):
    '''
    A method for adding pseudo lane information for the nextseq platform

    :param lanes: A list of pseudo lanes, default ['1','2','3','4']
    :returns:None
    '''
    try:
      lanes = list(lanes)
      # if self.get_platform_name() == 'NextSeq2000':
      #   lanes = ['1',]
      data = self._data
      newdata = list()
      for row in data:
        for lane in lanes:
          temp_row = copy.deepcopy(row)
          temp_row['PseudoLane'] = lane
          newdata.append(temp_row)
      self._data = newdata
    except:
      raise


  def _reformat_project_and_description(
        self, project_field='Sample_Project', description_field='Description'):
    '''
    A Function for removing the user information from Project field and
    converting ':' to '-' in the description field

    :param project_field: A column name for project lookup, default Sample_Project
    :param description_field: A column name for description lookup, default Description
    '''
    try:
      data = self._data
      for row in data:
        if project_field not in list(row.keys()):
          raise ValueError(
                  'project field {0} not found in sample sheet {1}'.\
                    format(project_field, self.infile))
        if description_field not in list(row.keys()):
          raise ValueError(
                  'description field {0} not found in sample sheet {1}'.\
                    format(description_field, self.infile))
        project = row[project_field].split(':')[0]
        row[project_field] = project
        description = row[description_field]
        row[description_field] = \
          description.replace(':','-').upper()
      self._data = data
    except:
      raise


  def get_reverse_complement_index(self, index_field='index2'):
    '''
    A function for changing the I5_index present in the index2 field of the
    samplesheet to intsreverse complement base

    :param index_field: Column name for index 2, default index2
    '''
    try:
      data = self._data
      for row in data:
        if index_field in list(row.keys()):
          # Only run the reverse complement function if index2 exists
          index = row[index_field]
          row[index_field] = \
            index.upper().\
              translate(
                str.maketrans('ACGT','TGCA'))[::-1]
      self._data = data
    except:
      raise


  def get_platform_name(self, section='Header'):
    '''
    Function for getting platform details from samplesheet header

    :param section: File section for lookup, default 'Header'
    '''
    try:
      if self.samplesheet_version == 'v1':
        field = 'Application'
      if self.samplesheet_version == 'v2':
        field = 'InstrumentType'
      header_section_data = \
        self._header_data[section]
      pattern = \
        re.compile('^{},'.format(field), re.IGNORECASE)
      match = 0
      for row in header_section_data:
        if re.search(pattern, row):
          match = 1
          (_, machine_name) = row.split(',')[0:2]
          return machine_name
      if match == 0:
        raise ValueError(
                'samplesheet {0} doesn\'t have the field {1}'.\
                  format(self.infile, field))
    except:
      raise


  def get_lane_count(self, lane_field='Lane', target_platforms=('HiSeq','NovaSeq')):
    '''
    Function for getting the lane information for HiSeq runs
    It will return 1 for both MiSeq and NextSeq runs

    :param lane_field: Column name for lane info, default 'Lane'
    :param target_platform: Hiseq platform tag, default 'HiSeq'
    :returns: A list of lanes present in samplesheet file
    '''
    try:
      data = self._data
      platform_name = self.get_platform_name()
      lane = set()
      match_count = 0
      for target_platform in target_platforms:
        pattern = \
          re.compile(
            '^{}'.format(target_platform),
            re.IGNORECASE)
        if re.search(pattern, platform_name) and \
           match_count == 0:
          match_count += 1
          for row in data:
            if lane_field not in list(row.keys()):
              raise ValueError(
                      'lane field {0} not found for platform, {1}, sample sheet {2}'.\
                        format(lane_field, target_platform, self.infile))
            lane.add(row[lane_field])
      if match_count == 0:
        lane.add(1)
      return list(lane)
    except:
      raise


  def check_sample_header(self, section, condition_key, return_values=False):
    '''
    Function for checking SampleSheet header

    :param section: A field name for header info check
    :param condition_key: A condition key for header info check
    :param return_values: Taggole for a list of return values instead of zero or match counts
    :returns: zero if its not present or number of occurrence of the term, or list of matching items with return_values=True
    '''
    try:
      header_data = self._header_data
      if not condition_key or not section:
        raise ValueError(
                'section and condition_key are required for sample header check')
      exists = list()
      pattern = \
        re.compile(
          '^{}$'.format(condition_key),
          re.IGNORECASE)
      exists = \
        [row for row in header_data[section] \
               if re.search(pattern, row.split(',')[0])]
      if return_values:
        return exists
      else:
        return len(exists)
    except:
      raise


  def modify_sample_header(self, section, type, condition_key, condition_value=''):
    '''
    Function for modifying SampleSheet header

    :param section: A field name for header info check
    :param condition_key: A condition key for header info check
    :param type: Mode type, 'add' or 'remove'
    :param condition_value: Its is required for 'add' type
    '''
    try:
      header_data = self._header_data
      if ( type.lower().strip() == 'add' ):
        # check if condition key is already present
        if (self.check_sample_header(section=section, condition_key=condition_key)):
           raise ValueError(
                  'condition_key {} already present for section {}'.\
                    format(condition_key, section))
        # can't use the default condition_value
        if not condition_value:
          raise ValueError(
                  'condition_value is required for type {} and key {}'.\
                    format(type, condition_key))
        else:
          header_data[section].\
            append('{0},{1}'.\
              format(condition_key, condition_value))
      elif ( type.lower().strip() == 'remove' ):
        filtered_header_section = list()
        pattern = \
          re.compile(
            '^{}$'.format(condition_key),
            re.IGNORECASE)
        for row in header_data[section]:
          if re.match( pattern, row.split(',')[0] ):
            pass
          else:
            filtered_header_section.append(row)
        header_data[section] = filtered_header_section
      else:
        raise ValueError('type {} not supported'.format(type))
      # resetting the header
      self._header_data = header_data
    except:
      raise

  def set_header_for_bclconvert_run(
      self, bases_mask, min_trimmed_length=8, mask_short_read=8):
    try:
      bclconv_settings = {
        'CreateFastqForIndexReads': 1,
        'MinimumTrimmedReadLength': min_trimmed_length,
        'FastqCompressionFormat': 'gzip',
        'MaskShortReads': mask_short_read,
        'OverrideCycles': bases_mask}
      settings_section = None
      if self.samplesheet_version == 'v1':
        settings_section = 'Settings'
      elif self.samplesheet_version == 'v2':
        settings_section = 'BCLConvert_Settings'
      header_data = self._header_data
      new_settings_data = list()
      settings_data = header_data.get(settings_section)
      if settings_data is not None:
        for s in settings_data:
          s = s.split(',')
          if s[0] != '' and s[1] != '':
            if s[0] in bclconv_settings:
              new_settings_data.\
                append(
                  '{0},{1}'.format(
                    s[0],
                    bclconv_settings.get(s[0])))
              del bclconv_settings[s[0]]
            else:
              new_settings_data.\
                append('{0},{1}'.format(s[0], s[1]))
        for key, val in bclconv_settings.items():
          new_settings_data.\
            append('{0},{1}'.format(key, val))
        header_data[settings_section] = new_settings_data
      else:
        new_settings_data = [
          '{0},{1}'.format(key, val)
            for key, val in bclconv_settings.items()]
        header_data.\
          update({settings_section: new_settings_data})
    except Exception as e:
      raise ValueError("Failed to set header for bclconvert run: {0}".format(e))


  def filter_sample_data(self, condition_key, condition_value, method='include',
                         lane_header='Lane', lane_default_val='1'):
    '''
    Function for filtering SampleSheet data based on matching condition

    :param condition_key: A samplesheet column name
    :param condition_value: A keyword present in the selected column
    :param method: 'include' or 'exclude' for adding or removing selected column from the samplesheet
                   default is include
    '''
    try:
      condition_value = str(condition_value).strip()
      raw_data = self._data
      filtered_data = list()
      for row in raw_data:
        if condition_key not in list(row.keys()):
          raise ValueError(
                  'key {}, value {} not found for {}'.\
                    format(condition_key,condition_value,row))
        else:
          if method == 'include':
            if row[condition_key].upper() == condition_value.upper():
              filtered_data.append(row)
          elif method == 'exclude':
            if row[condition_key].upper() != condition_value.upper():
              filtered_data.append(row)
          else:
            raise ValueError(
                    'method {0} not supported'.format(method))
      # resetting data information
      self._data = filtered_data
    except:
      raise


  def print_sampleSheet(self, outfile):
    '''
    Function for printing output SampleSheet

    :param outfile: A output samplesheet path
    '''
    try:
      header_data = self._header_data
      data_header = self._data_header
      data = self._data
      # check if output file exists
      if os.path.exists(outfile):
        raise IOError(
                'output file {} already present'.\
                  format(outfile))
      with open(outfile, 'w') as file:
        # formatting output
        for header_key in header_data.keys():
          file.write('[{}]\n'.format(header_key))
          header_data_section = '\n'.join(header_data[header_key])
          file.write('{}\n'.format(header_data_section))
        version = self.samplesheet_version
        if version == 'v1':
          file.write('[{}]\n'.format('Data'))
        elif version == 'v2':
          file.write('[{}]\n'.format('BCLConvert_Data'))
        file.write('{}\n'.format(','.join(data_header)))
        for row in data:
          data_row = list()
          for h in data_header:
            data_row.append(row[h])
          file.write('{}\n'.format(','.join(data_row)))
    except:
      raise


  def _load_header(self):
    '''
    Function for loading SampleSheet header
    Output: 2 lists , 1st list of column headers for data section,
            2nd list of dictionaries containing data
    '''
    try:
      sample_data = self._sample_data
      header_data = dict()
      for keys in sample_data:
        if keys not in self.data_header_name:
          header_data[keys] = sample_data[keys]
      return header_data
    except:
      raise


  def _load_data(self):
    '''
    Function for loading SampleSheet data
    '''
    try:
      sample_data = self._sample_data
      for entry in self.data_header_name:
        if entry in sample_data:
          data = sample_data[entry]
          if entry == 'Data':
            self._samplesheet_version = 'v1'
          elif entry == 'BCLConvert_Data':
            self._samplesheet_version = 'v2'
          else:
            self._samplesheet_version = 'unknown'
      data = deque(data)
      data_header = data.popleft()
      data_header = data_header.split(',')
      sample_data = list()
      for row in data:
        row = row.split(',')
        row = [
          row_val.rstrip() for row_val in row]
        row_data = \
          dict(zip(data_header,row))
        sample_data.append(row_data)
      return data_header, sample_data
    except:
      raise


  def _read_samplesheet(self):
    '''
    Function for reading SampleSheet.csv file
    '''
    try:
      infile = self.infile
      if os.path.exists(infile) == False:
        raise IOError('file {0} not found'.\
                      format(infile))
      sample_data = defaultdict(list)
      header = ''
      with open(infile, 'r') as f:
        for i in f:
          row = i.rstrip('\n')
          if row != '':
            if row.startswith('['):
              header = \
                row.split(',')[0].strip('[').strip(']')
            else:
              sample_data[header].append(row)
      return sample_data
    except:
      raise