import os, json
import pandas as pd
from jsonschema import validate, Draft4Validator
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.utils.gviz_utils import convert_to_gviz_json_for_display
from collections import defaultdict
from igf_data.utils.fileutils import check_file_path
from igf_data.process.metadata_reformat.reformat_metadata_file import EXPERIMENT_TYPE_LOOKUP

class Validate_project_and_samplesheet_metadata:
  '''
  A package for running validation checks for project and samplesheet metadata file
  
  :param samplesheet_file: A samplesheet input file
  :param metadata_files: A list of metadata input file
  :param samplesheet_schema: A json schema for samplesheet file validation
  :param metadata_schema: A json schema for metadata file validation
  '''
  def __init__(self,samplesheet_file,metadata_files,samplesheet_schema,
               metadata_schema,samplesheet_name='SampleSheet.csv'):
    self.samplesheet_file = samplesheet_file
    self.metadata_files = metadata_files
    self.samplesheet_schema = samplesheet_schema
    self.metadata_schema = metadata_schema
    self.samplesheet_name = samplesheet_name

  def get_samplesheet_validation_report(self):
    '''
    A method for running validation checks on input samplesheet file
    :returns: A list of errors or an empty list
    '''
    try:
      samplesheet = SampleSheet(infile=self.samplesheet_file)
      samplesheet_header_count = 0
      for _,val in samplesheet._header_data.items():
        samplesheet_header_count += 1+len(val)                                  # count samplesheet header lines

      samplesheet_header_count += 2                                             # for data and header
      with open(self.samplesheet_schema,'r') as jp:
        json_data = json.load(jp)

      samplesheet_json_fields = list(json_data['items']['properties'].keys())
      errors = list()
      for header_name in samplesheet._data_header:
        if header_name not in samplesheet_json_fields:
          errors.\
            append({
              'column':'',
              'line':'',
              'filename':os.path.basename(self.samplesheet_file),
              'error':'Header {0} is not supported. Validation incomplete.'.\
                      format(header_name)
            })
      if len(errors)>0:
          return errors                                                         # stop checking samplesheet

      if os.path.basename(self.samplesheet_file) != self.samplesheet_name:
        errors.\
          append({
            'column':'',
            'line':'',
            'filename':os.path.basename(self.samplesheet_file),
            'error':'samplesheet file should be {1}, name {0} is not supported.'.\
                    format(
                      os.path.basename(self.samplesheet_file),
                      self.samplesheet_name)
          })
      samplesheet_data = pd.DataFrame(samplesheet._data)
      for line,l_data in samplesheet_data.groupby(samplesheet_data.columns.tolist(),as_index=False):
        if len(l_data.index) > 1:
          errors.\
            append({
              'column':'',
              'line':'',
              'filename':os.path.basename(self.samplesheet_file),
              'error':'Duplicate entry:{0}, {1}'.\
                      format(
                        len(l_data.index),
                        line)
            })
      samplesheet._data = \
        samplesheet_data.\
        drop_duplicates().\
        to_dict(orient='records')
      samplesheet_errors = \
        samplesheet.\
        validate_samplesheet_data(\
          schema_json=self.samplesheet_schema)
      if len(samplesheet_errors)>0:
        errors.\
          extend([
            {'column':'',
              'line':'',
              'filename':os.path.basename(self.samplesheet_file),
              'error':err} 
             if isinstance(err,str) else
            {'column':err.schema_path[2],
             'line':err.path[0]+1+samplesheet_header_count,
             'filename':os.path.basename(self.samplesheet_file),
             'error':err.message}
            for err in samplesheet_errors
          ])
      index_lookup_list = ['index']
      if 'index2' in samplesheet._data_header:
        index_lookup_list.append('index2')
      if 'Lane' in samplesheet._data_header:
        for lane,l_data in pd.DataFrame(samplesheet._data).drop_duplicates().groupby('Lane'):
          for index, i_data in l_data.groupby(index_lookup_list):
            if(len(i_data)>1):
              errors.\
              append(\
                {'column':'',
                 'line':'',
                 'filename':os.path.basename(self.samplesheet_file),
                 'error':'Duplicate barcodes found, Index: {0}, lane: {1}'.\
                         format(index,
                                lane)}
              )
      else:
        for index, i_data in pd.DataFrame(samplesheet._data).drop_duplicates().groupby(index_lookup_list):
          if(len(i_data)>1):
              errors.\
              append(\
                {'column':'',
                 'line':'',
                 'filename':os.path.basename(self.samplesheet_file),
                 'error':'Duplicate barcodes found, Index: {0}'.\
                         format(index)}
              )

      duplicate_sample_entries = list()
      duplicate_igf_entries = list()
      data = \
        pd.DataFrame(samplesheet._data).\
        drop_duplicates()
      if 'Lane' in samplesheet._data_header:
        data_grp = \
          data.groupby(['Lane',
                        'Sample_Name'])
        for _,grp in data_grp:
          dup_sample = \
            grp[grp['Sample_Name'].\
            duplicated()]['Sample_Name']
          if len(dup_sample.index)>0:
            for entry in dup_sample.values:
              dup_data = \
                grp[grp['Sample_Name']==entry][['Lane',
                                                'Sample_ID',
                                                'Sample_Name']]
              duplicate_sample_entries.\
              append(dup_data.to_dict(orient='records'))

        data_igf_grp = \
          data.groupby(['Lane',
                        'Sample_ID'])
        for _,grp in data_igf_grp:
          dup_sample = \
            grp[grp['Sample_ID'].duplicated()]['Sample_ID']
          if len(dup_sample.index)>0:
            for entry in dup_sample.values:
              dup_data = \
                grp[grp['Sample_ID']==entry][['Lane',
                                              'Sample_ID',
                                              'Sample_Name']]
              duplicate_igf_entries.\
              append(dup_data.to_dict(orient='records'))
      else:
        data_grp = data.groupby(['Sample_Name'])
        for _,grp in data_grp:
          dup_sample = \
            grp[grp['Sample_Name'].duplicated()]['Sample_Name']
          if len(dup_sample.index)>0:
            for entry in dup_sample.values:
              dup_data = \
                grp[grp['Sample_Name']==entry][['Sample_ID',
                                                'Sample_Name']]
              duplicate_sample_entries.\
              append(dup_data.to_dict(orient='records'))

        data_igf_grp = data.groupby(['Sample_ID'])
        for key,grp in data_igf_grp:
          dup_sample = grp[grp['Sample_ID'].duplicated()]['Sample_ID']
          if len(dup_sample.index)>0:
            for entry in dup_sample.values:
              dup_data = \
                grp[grp['Sample_ID']==entry][['Sample_ID',
                                              'Sample_Name']]
              duplicate_igf_entries.\
              append(dup_data.to_dict(orient='records'))

      if len(duplicate_sample_entries)>0:
         errors.\
         extend(\
           [{'column':'Sample_Name',
             'line':'',
             'filename':os.path.basename(self.samplesheet_file),
             'error':'Duplicate sample name found: {0}'.format(err)}
             for err in duplicate_sample_entries])

      if len(duplicate_igf_entries)>0:
         errors.\
         extend(\
           [{'column':'Sample_ID',
             'line':'',
             'filename':os.path.basename(self.samplesheet_file),
             'error':'Duplicate sample IGF id found: {0}'.format(err)}
             for err in duplicate_igf_entries])

      return errors
    except:
      raise

  def get_metadata_validation_report(self):
    '''
    A method for running validation check on input metdata files
    :returns: A list of errors or an empty list
    '''
    try:
      error_list = list()
      with open(self.metadata_schema,'r') as jf:
        schema = json.load(jf)

      metadata_validator = Draft4Validator(schema)
      metadata_json_fields = list(schema['items']['properties'].keys())

      for metadata_file in self.metadata_files:
        check_file_path(metadata_file)
        metadata = pd.read_csv(metadata_file)
        for line,l_data in metadata.fillna('').groupby(metadata.columns.tolist(),as_index=False):
          if len(l_data.index) > 1:
            error_list.\
            append(\
              {'column':'',
               'line':'',
               'filename':os.path.basename(metadata_file),
               'error':'Duplicate entry:{0}, {1}'.\
                        format(len(l_data.index),
                               line)}
            )

        metadata = \
          metadata.\
          drop_duplicates()
        metadata_error_list = list()
        for header_name in metadata.columns:
          if not header_name.startswith('Unnamed') and \
             not header_name in metadata_json_fields:                           # ignore any column without a header
            metadata_error_list.\
            append(\
              {'column':'',
               'line':'',
               'filename':os.path.basename(metadata_file),
               'error':'Header {0} is not supported. Validation incomplete.'.\
                       format(header_name)}
            )
        if len(metadata_error_list)>0:
          error_list.\
          extend(metadata_error_list)
          continue                                                              # skip validation check for metadata file

        library_errors = list()
        library_errors = \
          (metadata.apply(lambda x: \
            self.check_metadata_library_by_row(data=x),
            axis=1,
            result_type=None))                                                  # check metadata per row
        library_errors = \
          [i for i in library_errors
             if i is not None]                                                  # filter library errors
        if len(library_errors)>0:
          library_errors = \
            [{'column':'',
              'line':'',
              'filename':os.path.basename(metadata_file),
              'error':err}
              for err in library_errors]                                        # reformat library errors
          error_list.extend(library_errors)                                     # add library errors to the list of final errors

        metadata = \
          metadata.\
          fillna("").\
          applymap(lambda x: str(x))
        if 'taxon_id' in metadata.columns:
          metadata['taxon_id'] = \
            metadata['taxon_id'].\
            astype(str)

        json_data = \
          metadata.\
          to_dict(orient='records')
        errors = \
          sorted(metadata_validator.iter_errors(json_data),
                 key=lambda e: e.path)
        errors = \
          [{'column':'',
            'line':'',
            'filename':os.path.basename(metadata_file),
            'error':err} 
           if isinstance(err,str) else 
           {'column':err.schema_path[2],
            'line':err.path[0]+1,
            'filename':os.path.basename(metadata_file),
            'error':err.message} 
           for err in errors]
        error_list.extend(errors)
      return error_list
    except:
      raise

  def get_merged_errors(self):
    '''
    A method for running the validation checks on input samplesheet metadata and samplesheet files
    :returns: A list of errors or an empty list
    '''
    try:
      all_errors = list()
      all_errors.extend(self.get_samplesheet_validation_report())
      all_errors.extend(self.get_metadata_validation_report())
      all_errors.extend(self.compare_metadata())
      return all_errors
    except:
      raise

  def dump_error_to_csv(self,output_csv):
    '''
    A method for dumping list or errors to a csv file
    :returns: output csv file path if any errors found, or else None
    '''
    try:
      all_errors = self.get_merged_errors()
      if len(all_errors)==0:
        return None
      else:
        all_errors = pd.DataFrame(all_errors)
        if os.path.exists(output_csv):
          raise IOError('Output file {0} already present'.format(output_csv))

        all_errors.\
          to_csv(output_csv,index=False)
        return output_csv
    except:
      raise

  def convert_errors_to_gviz(self,output_json=None):
    '''
    A method for converting the list of errors to gviz format json
    
    :param output_json: A output json file for saving data, default None
    :returns: A gviz json data block for the html output if output_json is None,
              or else None
    '''
    try:
      all_errors = self.get_merged_errors()
      if len(all_errors)==0:
        description = {'error': ('string', 'Error')}
        columns_order = ['error']
        data = [{'error':'No error found'}]
        json_data = \
          convert_to_gviz_json_for_display(
            description=description,
            data=data,
            columns_order=columns_order)
      else:
        description = \
          {'filename':('string', 'File name'),
           'column':('string', 'Column name'),
           'line' : ('string', 'Line No.'),
           'error': ('string', 'Error')
          }
        columns_order = \
          ['filename',
           'column',
           'line',
           'error']
        json_data = \
          convert_to_gviz_json_for_display(
            description=description,
            data=all_errors,
            columns_order=columns_order)
      if output_json is None:
        return json_data
      else:
        with open(output_json,'w') as jf:
          jf.write(json_data)
        return None
    except:
      raise

  @staticmethod
  def check_metadata_library_by_row(data):
    '''
    A static method for checking library type metadata per row
    
    :param data: A pandas data series containing sample metadata
    :returns: An error message or None
    '''
    try:
      err = None
      if 'sample_igf_id' not in data:
        err = 'Sample igf id not found'
      else:
        if 'library_source' in data and \
           'library_strategy' in data and \
           'experiment_type' in data:
          err = \
            Validate_project_and_samplesheet_metadata.\
              validate_metadata_library_type(
                sample_id=data.get('sample_igf_id'),
                library_source=data.get('library_source'),
                library_strategy=data.get('library_strategy'),
                experiment_type=data.get('experiment_type'))

      return err
    except:
      raise

  @staticmethod
  def validate_metadata_library_type(sample_id,library_source,library_strategy,
                                     experiment_type):
    '''
    A staticmethod for validating library metadata information for sample
    
    :param sample_id: Sample name
    :param library_source: Library source information
    :param library_strategy: Library strategy information
    :param experiment_type: Experiment type information
    :returns: A error message string or None
    '''
    try:
      error_msg = None
      exp_lookup_data = pd.DataFrame(EXPERIMENT_TYPE_LOOKUP)
      if library_source == 'GENOMIC':
        library_strategy_list = \
          list(exp_lookup_data[exp_lookup_data['library_source']=='GENOMIC']['library_strategy'].values)
        library_strategy_list.append('UNKNOWN')
        experiment_type_list = \
          list(exp_lookup_data[exp_lookup_data['library_source']=='GENOMIC']['experiment_type'].values)
        experiment_type_list.append('UNKNOWN')
        if library_strategy not in library_strategy_list or \
            experiment_type not in experiment_type_list:
          error_msg = \
            '{0}: library_strategy {1} or experiment_type {2} is not compatible with library_source {3}'.\
            format(sample_id,
                   library_strategy,
                   experiment_type,
                   library_source)
      elif library_source == 'TRANSCRIPTOMIC':
        library_strategy_list = \
          list(exp_lookup_data[exp_lookup_data['library_source']=='TRANSCRIPTOMIC']['library_strategy'].values)
        library_strategy_list.append('UNKNOWN')
        experiment_type_list = \
          list(exp_lookup_data[exp_lookup_data['library_source']=='TRANSCRIPTOMIC']['experiment_type'].values)
        experiment_type_list.append('UNKNOWN')
        if library_strategy not in library_strategy_list or \
           experiment_type not in experiment_type_list:
          error_msg = \
            '{0}: library_strategy {1} or experiment_type {2} is not compatible with library_source {3}'.\
            format(sample_id,
                   library_strategy,
                   experiment_type,
                   library_source)
      elif library_source == 'GENOMIC_SINGLE_CELL':
        library_strategy_list = \
          list(exp_lookup_data[exp_lookup_data['library_source']=='GENOMIC_SINGLE_CELL']['library_strategy'].values)
        library_strategy_list.append('UNKNOWN')
        experiment_type_list = \
          list(exp_lookup_data[exp_lookup_data['library_source']=='GENOMIC_SINGLE_CELL']['experiment_type'].values)
        experiment_type_list.append('UNKNOWN')
        if library_strategy not in library_strategy_list or \
           experiment_type not in experiment_type_list:
          error_msg = \
            '{0}: library_strategy {1} or experiment_type {2} is not compatible with library_source {3}'.\
            format(sample_id,
                   library_strategy,
                   experiment_type,
                   library_source)
      elif library_source == 'TRANSCRIPTOMIC_SINGLE_CELL':
        library_strategy_list = \
          list(exp_lookup_data[exp_lookup_data['library_source']=='TRANSCRIPTOMIC_SINGLE_CELL']['library_strategy'].values)
        library_strategy_list.append('UNKNOWN')
        experiment_type_list = \
          list(exp_lookup_data[exp_lookup_data['library_source']=='TRANSCRIPTOMIC_SINGLE_CELL']['experiment_type'].values)
        experiment_type_list.append('UNKNOWN')
        if library_strategy not in library_strategy_list or \
           experiment_type not in experiment_type_list:
          error_msg = \
            '{0}: library_strategy {1} or experiment_type {2} is not compatible with library_source {3}'.\
            format(sample_id,
                   library_strategy,
                   experiment_type,
                   library_source)

      return error_msg
    except:
      raise

  def compare_metadata(self):
    '''
    A function for comparing samplesheet and metadata files
    
    :returns: A list of error or an empty list
    '''
    try:
      errors = list()
      # read metadata file to data structure
      metadata_data = \
        defaultdict(lambda: defaultdict(lambda: set()))

      for metadata_file in self.metadata_files:
        metadata = pd.read_csv(metadata_file)
        for row in metadata.to_dict(orient='records'):
          project = row['project_igf_id']
          sample = row['sample_igf_id']
          email = row['email_id']
          user = row['name']
          metadata_data[project]['sample'].add(sample)
          metadata_data[project]['email'].add(email)
          metadata_data[project]['user'].add(user)

      # read samplesheet to data structure
      samplesheet_data = defaultdict(lambda: defaultdict(lambda: set()))
      sa = SampleSheet(infile=self.samplesheet_file)
      for row in sa._data:
        project = row['Sample_Project']
        sample = row['Sample_ID']
        samplesheet_data[project]['sample'].add(sample)

      # count project and check difference
      if len(metadata_data.keys()) != len(samplesheet_data.keys()):
        errors.\
          append({
            'column':'Sample_Project',
            'line':'',
            'filename':os.path.basename(self.samplesheet_file),
            'error':'Metadata files have {0} projects, samplesheet has {1} projects'.\
                    format(
                      len(metadata_data.keys()),
                      len(samplesheet_data.keys()))
          })                                                                    # project counts are not matching between samplesheet and metadata

      non_matching_projects = \
        list(set(samplesheet_data.keys()).\
             difference(set(metadata_data.keys())))                             # look for missing project metadata
      if len(non_matching_projects)>0:
        errors.\
          append({
            'column':'Sample_Project',
            'line':'',
            'filename':os.path.basename(self.samplesheet_file),
            'error':'Metadata missing for following projects: {0}'.\
                    format(non_matching_projects)
          })                                                                    # project names are not matching

      # count sample and check difference
      for project in samplesheet_data.keys():
        metadata_samples = set()
        samplesheet_samples = samplesheet_data.get(project).get('sample')
        metadata_project = metadata_data.get(project)
        if metadata_project is not None:
          metadata_samples = metadata_data.get(project).get('sample')

        if len(metadata_samples) != len(samplesheet_samples):
          errors.\
            append({
              'column':'Sample_ID',
              'line':'',
              'filename':os.path.basename(self.samplesheet_file),
              'error':'Metadata files have {0} samples, samplesheet has {1} samples, for project {2}'.\
                      format(
                        len(metadata_samples),
                        len(samplesheet_samples),
                        project)
            })                                                                  # check for sample count mismatch
        non_matching_samples = \
          list(samplesheet_samples.\
               difference(metadata_samples))
        if len(non_matching_samples)>0:
          errors.\
            append({
              'column':'Sample_ID',
              'line':'',
              'filename':os.path.basename(self.samplesheet_file),
              'error':'Metadata missing for {0} samples for project {1}. Small list {2}'.\
                      format(
                        len(non_matching_samples),
                        project,
                        non_matching_samples \
                          if len(non_matching_samples)<5 \
                            else non_matching_samples[0:5])
            })                                                                  # printing top 5 non matching samples

      # count project user and email
      for project in metadata_data.keys():
        users = metadata_data.get(project).get('user')
        email = metadata_data.get(project).get('email')
        if len(users)>1 or len(email)>1:
          errors.\
            append({
              'column':'name / email_id',
              'line':'',
              'filename':' ,'.join([os.path.basename(metadata_file)
                                     for metadata_file in self.metadata_files]),
              'error':'Metadata files have {0} users and {1} email_ids, for project {2}'.\
                      format(
                        len(users),
                        len(email),
                        project)
            })                                                                  # check for project user info

      return errors
    except:
      raise