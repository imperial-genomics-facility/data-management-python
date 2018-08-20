import os, json
import pandas as pd
from jsonschema import validate, Draft4Validator
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.utils.gviz_utils import convert_to_gviz_json_for_display

class Validate_project_and_samplesheet_metadata:
  '''
  A package for running validation checks for project and samplesheet metadata file
  
  :param samplesheet_file: A samplesheet input file
  :param metadata_files: A list of metadata input file
  :param samplesheet_schema: A json schema for samplesheet file validation
  :param metadata_schema: A json schema for metadata file validation
  '''
  def __init__(self,samplesheet_file,metadata_files,samplesheet_schema,metadata_schema):
    self.samplesheet_file=samplesheet_file
    self.metadata_files=metadata_files
    self.samplesheet_schema=samplesheet_schema
    self.metadata_schema=metadata_schema

  def get_samplesheet_validation_report(self):
    '''
    A method for running validation checks on input samplesheet file
    :returns: A list of errors or an empty list
    '''
    try:
      samplesheet=SampleSheet(infile=self.samplesheet_file)
      samplesheet_errors=samplesheet.validate_samplesheet_data(schema_json=self.samplesheet_schema)
      with open(self.samplesheet_schema,'r') as jp:
        json_data=json.load(jp)

      samplesheet_json_fields=list(json_data['items']['properties'].keys())
      errors=list()

      if header_name in samplesheet._data_header:
        if header_name not in samplesheet_json_fields:
          errors.append({'column':'',
                         'line':'',
                         'filename':os.path.basename(self.samplesheet_file),
                         'error':'Header {0} is not supported. Validation incomplete.'.\
                         format(header_name)}
                       )
        if len(errors)>0:
          return errors                                                         # stop checking samplesheet

      if len(samplesheet_errors)>0:
        errors=[{'column':'',
                 'line':'',
                 'filename':os.path.basename(self.samplesheet_file),
                 'error':err} 
                 if isinstance(err,str) else 
                {'column':err.schema_path[2],
                 'line':err.path[0]+1,
                 'filename':os.path.basename(self.samplesheet_file),
                 'error':err.message}
                for err in samplesheet_errors]
      return errors
    except:
      raise

  def get_metadata_validation_report(self):
    '''
    A method for running validation check on input metdata files
    :returns: A list of errors or an empty list
    '''
    try:
      error_list=list()
      with open(self.metadata_schema,'r') as jf:
        schema=json.load(jf)
      metadata_validator=Draft4Validator(schema)
      metadata_json_fields=list(schema['items']['properties'].keys())

      for metadata_file in self.metadata_files:
        metadata=pd.read_csv(metadata_file)
        metadata_error_list=list()
        for header_name in metadata.columns:
          if not header_name.startswith('Unnamed') and \
             not header_name in metadata_json_fields:                           # ignore any column without a header
            metadata_error_list.append({'column':'',
                                        'line':'',
                                        'filename':os.path.basename(metadata_file),
                                        'error':'Header {0} is not supported. Validation incomplete.'.\
                                        format(header_name)}
                                      )
        if len(metadata_error_list)>0:
          error_list.extend(metadata_error_list)
          continue                                                              # skip validation check for metadata file

        metadata=metadata.fillna("").applymap(lambda x: str(x))
        if 'taxon_id' in metadata.columns:
          metadata['taxon_id']=metadata['taxon_id'].astype(str)

        json_data=metadata.to_dict(orient='records')
        errors=sorted(metadata_validator.iter_errors(json_data), key=lambda e: e.path)
        errors=[{'column':'',
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
      all_errors=list()
      all_errors.extend(self.get_samplesheet_validation_report())
      all_errors.extend(self.get_metadata_validation_report())
      return all_errors
    except:
      raise

  def dump_error_to_csv(self,output_csv):
    '''
    A method for dumping list or errors to a csv file
    :returns: output csv file path if any errors found, or else None
    '''
    try:
      all_errors=self.get_merged_errors()
      if len(all_errors)==0:
        return None
      else:
        all_errors=pd.DataFrame(all_errors)
        if os.path.exists(output_csv):
          raise IOError('Output file {0} already present'.format(output_csv))

        all_errors.to_csv(output_csv,index=False)
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
      all_errors=self.get_merged_errors()
      if len(all_errors)==0:
        description={'error': ('string', 'Error')}
        columns_order=['error']
        data=[{'error':'No error found'}]
        json_data=convert_to_gviz_json_for_display(\
                    description=description,
                    data=data,
                    columns_order=columns_order
                  )
      else:
        description={'filename':('string', 'File name'),
                     'column':('string', 'Column name'),
                     'line' : ('string', 'Line No.'),
                     'error': ('string', 'Error')
                    }
        columns_order=['filename','column','line','error']
        json_data=convert_to_gviz_json_for_display(description=description,
                                                   data=all_errors,
                                                   columns_order=columns_order
                                                  )
      if output_json is None:
        return json_data
      else:
        with open(output_json,'w') as jf:
          jf.write(json_data)
        return None
    except:
      raise