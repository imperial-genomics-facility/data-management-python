import re
import pandas as pd
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.utils.sequtils import rev_comp
from igf_data.process.metadata_reformat.reformat_metadata_file import Reformat_metadata_file

class Reformat_samplesheet_file:
  '''
  A class for reformatting samplesheet file

  :param infile: Input samplesheet file
  :param remove_adapters: A toggle for removing adapters from header section ,default False
  :param revcomp_index1: A toggle for reverse complementing index1 column, default False
  :param revcomp_index2: A toggle for reverse complementing index2 column, default False
  :param tenx_label: Description label for 10x experiments, default '10X'
  :param sample_id: Sample id column name, default 'Sample_ID'
  :param sample_name: Sample name column name, default 'Sample_Name'
  :param index: I7 index column name, default 'index'
  :param index2: I5 index column name, default 'index2'
  :param sample_project: Project name column name, default 'Sample_Project'
  :param description: Description column name, default 'Description'
  :param adapter_section: Adapter section name in header, default 'Settings'
  :param adapter_keys: A list of adapter keys to be removed from samplesheet header, default ('Adapter','AdapterRead2')
  '''
  def __init__(self,infile,
               remove_adapters=False,
               revcomp_index1=False,
               revcomp_index2=False,
               tenx_label='10X',
               sample_id='Sample_ID',
               sample_name='Sample_Name',
               index='index',
               index2='index2',
               sample_project='Sample_Project',
               description='Description',
               adapter_section='Settings',
               adapter_keys=('Adapter','AdapterRead2')):
    self.infile = infile
    self.tenx_label = tenx_label
    self.remove_adapters = remove_adapters
    self.revcomp_index1 = revcomp_index1
    self.revcomp_index2 = revcomp_index2
    self.sample_id = sample_id
    self.sample_name = sample_name
    self.index = index
    self.index2 = index2
    self.sample_project = sample_project
    self.description = description
    self.adapter_section = adapter_section
    self.adapter_keys = adapter_keys
        
  @staticmethod
  def detect_tenx_barcodes(index,tenx_label='10X'):
    '''
    A static method for checking 10X I7 index barcodes

    :param index: I7 index string
    :param tenx_label: A string description for 10X samples, default, '10X'
    :returns: A string
    '''
    try:
      description = ''
      pattern = re.compile(r'SI-[GN]A-[A-H]\d+',re.IGNORECASE)
      if re.match(pattern,index):
        description = tenx_label
      return description
    except Exception as e:
      raise ValueError('Failed to detect Tenx single cell barcode for index {0}, error: {1}'.format(index,e))

  def correct_samplesheet_data_row(self,row):
    '''
    A method for correcting samplesheet data row

    :param row: A Pandas Series
    :returns: A Pandas Series
    '''
    try:
      if not isinstance(row,pd.Series):
        raise TypeError('Expecting A pandas series and got {0}'.format(type(row)))

      if self.sample_id in row.keys():
        row[self.sample_id] = \
        Reformat_metadata_file.\
          sample_and_project_reformat(row[self.sample_id])                      # refoemat sample id

      if self.sample_project in row.keys():
        row[self.sample_project] = \
        Reformat_metadata_file.\
          sample_and_project_reformat(row[self.sample_project])                 # refoemat project name

      if self.sample_name in row.keys():
        row[self.sample_name] = \
        Reformat_metadata_file.\
          sample_name_reformat(row[self.sample_name])                           # refoemat sample name

      if self.index in row.keys() and \
         self.description in row.keys():
        row[self.description] = \
          self.detect_tenx_barcodes(\
            index=row[self.index],
            tenx_label=self.tenx_label)                                         # add description label for 10x samples

      if self.index in row.keys() and \
         self.description in row.keys() and \
         (row[self.index]!='' or row[self.index] is not None ) and \
          row[self.description] != self.tenx_label:
        row[self.index] = row[self.index].upper()
        if self.revcomp_index1:
          row[self.index] = rev_comp(row[self.index])                           # revcomp index 1

      if self.index2 in row.keys() and \
         (row[self.index2]!='' or row[self.index2] is not None ):
        row[self.index2] = row[self.index2].upper()
        if self.revcomp_index2:
          row[self.index2] = rev_comp(row[self.index2])                         # revcomp index 2

      if self.description in row.keys() and \
         (row[self.description] !='' or \
          row[self.description] is not None):
        row[self.description] = row[self.description].upper()                   # change description to upper case letters
      return row
    except Exception as e:
      raise ValueError('Failed to correct samplesheet data row {0},error {1}'.format(row,e))

  def reformat_raw_samplesheet_file(self,output_file):
    '''
    A method for refoematting raw samplesheet file

    :param output_file: An output file path
    :returns: None
    '''
    try:
      samplesheet = SampleSheet(infile=self.infile)
      samplesheet_data = pd.DataFrame(samplesheet._data)
      samplesheet_data.fillna('',inplace=True)
      samplesheet_data = \
        samplesheet_data.\
        apply(\
          lambda row: self.correct_samplesheet_data_row(row=row),
          axis=1,
          result_type='reduce')                                                 # refoemat samplesheet data
      samplesheet._data = \
        samplesheet_data.\
        to_dict(orient='records')                                               # update samplesheet object with new data
      if self.remove_adapters:
        for adapter_key in self.adapter_keys:
          samplesheet.\
          modify_sample_header(\
            section=self.adapter_section,
            type='remove',
            condition_key=adapter_key)                                          # remove adapters from samplesheet
      samplesheet.print_sampleSheet(outfile=output_file)                        # print corrected samplesheet
    except Exception as e:
      raise ValueError('Failed to reformat samplesheet file {0}, error {1}'.format(self.infile,e))