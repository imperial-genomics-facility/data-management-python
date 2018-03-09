import os, json
from igf_data.illumina.samplesheet import SampleSheet

class ProcessSingleCellSamplesheet:
  '''
  A class for processing samplesheet containing single cell (10X) index barcodes
  It requires a json format file listing all the single
  cell barcodes downloaded from this page
  https://support.10xgenomics.com/single-cell-gene-expression/sequencing/doc/
  specifications-sample-index-sets-for-single-cell-3
  
  required params:
  samplesheet_file: A samplesheet containing single cell samples
  singlecell_barcode_json: A JSON file listing single cell indexes
  singlecell_tag: A text keyword for the single cell sample description
  index_column: Column name for index lookup, default 'index'
  sample_id_column: Column name for sample_id lookup, default 'Sample_ID'
  sample_name_column: Column name for sample_name lookup, default 'Sample_NAme'
  orig_sample_id: Column name for keeping original sample ids, default 'Original_Sample_ID'
  orig_sample_name: Column name for keeping original sample_names, default: 'Original_Sample_Name'
  orig_index: Column name for keeping original index, default 'Original_index'
  '''
  
  def __init__(self,samplesheet_file,singlecell_barcode_json,\
               singlecell_tag='10X',index_column='index',\
               sample_id_column='Sample_ID', sample_name_column='Sample_Name',
               orig_sample_id='Original_Sample_ID', orig_sample_name='Original_Sample_Name',
               sample_description_column='Description',orig_index='Original_index'):
    self.samplesheet_file=samplesheet_file
    self.singlecell_barcode_json=singlecell_barcode_json
    self.index_column=index_column
    self.sample_id_column=sample_id_column
    self.sample_name_column=sample_name_column
    self.sample_description_column=sample_description_column
    self.singlecell_barcodes=ProcessSingleCellSamplesheet.\
                             _get_index_data(singlecell_barcode_json)           # get single cell indexes
    self.singlecell_tag=singlecell_tag
    self.orig_sample_id=orig_sample_id
    self.orig_sample_name=orig_sample_name
    self.orig_index=orig_index


  @staticmethod
  def _get_index_data(singlecell_barcode_json):
    '''
    An internal static method for reading single cell index data json files
    
    required param:
    singlecell_barcode_json: A JSON file containing single cell barcodes
    
    returns: A dictionary
    '''
    with open(singlecell_barcode_json) as json_data:
      index_json=json.load(json_data)

    index_data=dict()
    for index_line in index_json:
      index_name,index_list=index_line
      index_data[index_name]=index_list
    return index_data


  def _process_samplesheet_lines(self,data):
    '''
    An internal method for processing single cell indexes. Four lines of sample
    information are added to the output for each of the single cell samples
    
    required params:
    data: A dictionary containing data of single line of samplesheet
    
    returns a list of samplesheet data
    '''
    try:
      if not isinstance(data,dict):
        raise ValueError('expecting a dictionary and got {0}'.format(type(data)))
      if data[self.sample_description_column]==self.singlecell_tag:
        if self.index_column in data:
          sc_index=data[self.index_column]
          index_data=self.singlecell_barcodes
          if sc_index in index_data:
            final_data=list()
            suffix=0
            for index_seq in index_data[sc_index]:
              suffix +=1
              mod_data=dict(data)
              mod_data[self.orig_index]=mod_data[self.index_column]
              mod_data[self.orig_sample_id]=mod_data[self.sample_id_column]
              mod_data[self.orig_sample_name]=mod_data[self.sample_name_column]   # keep original sample infos
              mod_data[self.index_column]=index_seq                               # add sc index
              mod_data[self.sample_id_column]='{0}_{1}'.\
                                              format(mod_data[self.sample_id_column],\
                                                     suffix)                     # add sc sample id
              mod_data[self.sample_name_column]='{0}_{1}'.\
                                                format(mod_data[self.sample_name_column],\
                                                       suffix)                    # add sc sample name

              final_data.append(mod_data)
          else:
            raise ValueError('index {0} not found in file {1}'.\
                             format(sc_index,self.singlecell_barcode_json))
        else:
          raise ValueError('index column {0} not found in samplesheet data'.\
                           format(index_column))
        data=final_data
      else:
        data[self.orig_index]=''
        data[self.orig_sample_id]=''
        data[self.orig_sample_name]=''
      return data
    except:
      raise


  def change_singlecell_barcodes(self,output_samplesheet):
    '''
    A method for replacing single cell index codes present in the samplesheet 
    with the four index sequences. This method will create 4 samplesheet entries
    for each of the single cell samples with _1 to _4 suffix and relevant indexes
    
    required params:
    output_samplesheet: A file name of the output samplesheet
    '''
    try:
      #samplesheet=SampleSheet(infile=self.samplesheet_file)
      #samplesheet.filter_sample_data(condition_key=self.sample_description_column, 
      #                               condition_value=self.singlecell_tag, 
      #                               method='exclude')                          # filter single cell samples
      
      samplesheet_sc=SampleSheet(infile=self.samplesheet_file)
      #samplesheet_sc.filter_sample_data(condition_key=self.sample_description_column, 
      #                                  condition_value=self.singlecell_tag, 
      #                                  method='include')
      new_samplesheet_data=list()
      #new_samplesheet_data.extend(samplesheet._data)
      if len(samplesheet_sc._data) > 0:                                         # single cell samples are present
        for data in samplesheet_sc._data:
          processed_data=self._process_samplesheet_lines(data)
          if isinstance(processed_data,list):
            new_samplesheet_data.extend(processed_data)
          else:
            new_samplesheet_data.append(processed_data)

      #data_header=samplesheet._data_header
      data_header=samplesheet_sc._data_header
      #print(samplesheet_sc._data_header)
      #samplesheet._data_header=data_header.extend([self.orig_index,
      #                                             self.orig_sample_id,
      #                                             self.orig_sample_name])      # added new column names
      samplesheet_sc._data_header.extend([self.orig_index,
                          self.orig_sample_id,
                          self.orig_sample_name])   # added new column names
      #print(data_header)
      #samplesheet_sc._data_header=data_header
      #samplesheet._data=new_samplesheet_data                                    # add modified single cell records
      #samplesheet.print_sampleSheet(outfile=output_samplesheet)                 # write modified samplesheet
      samplesheet_sc._data=new_samplesheet_data                                    # add modified single cell records
      samplesheet_sc.print_sampleSheet(outfile=output_samplesheet)                 # write modified samplesheet
    except:
      raise