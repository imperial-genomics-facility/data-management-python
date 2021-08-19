import os,json
import pandas as pd
from igf_data.utils.fileutils import check_file_path,read_json_data
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.utils.sequtils import rev_comp

class ProcessSingleCellDualIndexSamplesheet:
  '''
  A class for processing singlecell dual indeices on the samplesheet

  :param samplesheet_file: A samplesheet file containg the single cell sample
  :param singlecell_dual_index_barcode_json: A json file containing 10x 3.1 dual index barcodes
  :param platform: Sequencing platform name
  :param singlecell_tag: A text keyword for the single cell sample Description, default '10X'
  :param index_column: Column name for index lookup, default 'index'
  :param index2_column: Column name for index2 lookup, default 'index2'
  :param sc_barcode_index1_tag: Index I7 tag in the json barcode file, default 'index(i7)'
  :param sample_description_column: Sample description column name in samplesheet, default 'Description'
  :param index2_rule: Rule for changing index2 barcode, default None
  :param workflow_group: A dictionary containing the I5 index tag for different platforms, default
                          (('HISEQ4000', 'index2_workflow_b(i5)'),
                           ('NEXTSEQ', 'index2_workflow_b(i5)'),
                           ('NOVASEQ6000', 'index2_workflow_a(i5)'),
                           ('MISEQ', 'index2_workflow_a(i5)'))
  '''
  def __init__(
    self,samplesheet_file,singlecell_dual_index_barcode_json,platform,singlecell_tag='10X',
    index_column='index',index2_column='index2',sample_description_column='Description',
    sc_barcode_index1_tag='index(i7)',index2_rule=None,
    workflow_group=(('HISEQ4000', 'index2_workflow_b(i5)'),
                    ('NEXTSEQ', 'index2_workflow_b(i5)'),
                    ('NOVASEQ6000', 'index2_workflow_a(i5)'),
                    ('MISEQ', 'index2_workflow_a(i5)'))):
    self.samplesheet_file = samplesheet_file
    self.singlecell_barcodes = \
      read_json_data(singlecell_dual_index_barcode_json)[0]
    self.platform = platform
    self.singlecell_tag = singlecell_tag
    self.index_column = index_column
    self.index2_column = index2_column
    self.sample_description_column = sample_description_column
    self.workflow_group = workflow_group
    self.sc_barcode_index1_tag = sc_barcode_index1_tag
    self.index2_rule = index2_rule

  def modify_samplesheet_for_sc_dual_barcode(
        self,output_samplesheet, remove_adapters=True, adapter_trim_section='Settings',
        adapter1_label='Adapter', adapter2_label='AdapterRead2'):
    '''
    A method for modifying samplesheet file sor sc dual index barcodes

    :param output_samplesheet: A file path for output samplesheet file. This file shouldn't be present.
    :param remove_adapters: Remove adapter config from samplesheet, default True
    :param adapter_trim_section: Adapter trim section name on samplesheet, default 'Settings'
    :param adapter1_label: Adapter 1 label, default 'Adapter'
    :param adapter2_label: Adapter 2 label, default 'AdapterRead2'
    '''
    try:
      if isinstance(self.workflow_group,tuple):
        self.workflow_group = dict(self.workflow_group)
      if not isinstance(self.workflow_group,dict):
        raise TypeError('Expecting a dictionary of workflow group, got {0}'.\
                format(self.workflow_group))
      if self.platform not in self.workflow_group:
        raise KeyError('Missing workflow type for platform {0}'.\
                format(self.platform))
      sa = SampleSheet(self.samplesheet_file)
      df = pd.DataFrame(sa._data)
      df[self.sample_description_column] = \
        df[self.sample_description_column].map(lambda x: x.strip().upper())             # convert sample description to upper case
      df = \
        df.apply(
          lambda series: self._replace_sc_dual_barcodes(series),
          result_type='reduce',
          axis=1)
      sa._data = df.to_dict(orient='records')
      if remove_adapters:
        adapter1_count = \
          sa.check_sample_header(
            section=adapter_trim_section,
            condition_key=adapter1_label)
        adapter2_count = \
          sa.check_sample_header(
            section=adapter_trim_section,
            condition_key=adapter2_label)
        if adapter1_count > 0:
          sa.modify_sample_header(
            section=adapter_trim_section,
            type='remove',
            condition_key=adapter1_label)                                       # remove adapter 1, if its present
        if adapter2_count > 0:
          sa.modify_sample_header(
            section=adapter_trim_section,
            type='remove',
            condition_key=adapter2_label)                                       # remove adapter 2, if its present
      sa.print_sampleSheet(output_samplesheet)
    except Exception as e:
      raise ValueError('Failed to convert samplesheet {0}, error: {1}'.\
              format(self.samplesheet_file,e))

  def _replace_sc_dual_barcodes(self,series):
    '''
    An internal method for replacing sc dual index barcodes from the dataframe

    :param series: Samplesheet row as Pandas Series
    :returns: Samplesheet row as Pandas Series
    '''
    try:
      if series[self.sample_description_column].strip() == self.singlecell_tag and \
         series[self.index_column] in self.singlecell_barcodes:
        index_barcode = series[self.index_column]
        index2_tag = self.workflow_group[self.platform]
        index_entry = self.singlecell_barcodes.get(index_barcode)
        if index_entry is None:
          raise KeyError('Failed to fetch sc dual index barcode {0} from list'.\
                  format(index_barcode))
        else:
          index1_seq = index_entry.get(self.sc_barcode_index1_tag)
          index2_seq = index_entry.get(index2_tag)
          if index1_seq is None or \
             index2_seq is None:
            raise KeyError('Correct index info not found for dual index barcode {0}'.\
                    format(index_barcode))
          series[self.index_column] = index1_seq
          series[self.sample_description_column] = ''
          if self.index2_rule is not None and \
             self.index2_rule == 'REVCOMP':
            series[self.index2_column] = rev_comp(index2_seq)
          else:
            series[self.index2_column] = index2_seq
      return series
    except Exception as e:
      raise ValueError('Failed sc dual index conversion, error: {0}'.\
              format(e))



class ProcessSingleCellSamplesheet:
  '''
  A class for processing samplesheet containing single cell (10X) index barcodes
  It requires a json format file listing all the single
  cell barcodes downloaded from this page
  https://support.10xgenomics.com/single-cell-gene-expression/sequencing/doc/
  specifications-sample-index-sets-for-single-cell-3

  :param samplesheet_file: A samplesheet containing single cell samples
  :param singlecell_barcode_json: A JSON file listing single cell indexes
  :param singlecell_tag: A text keyword for the single cell sample description
  :param index_column: Column name for index lookup, default 'index'
  :param sample_description_column: Sample description column name in samplesheet, default 'Description'
  :param sample_id_column: Column name for sample_id lookup, default 'Sample_ID'
  :param sample_name_column: Column name for sample_name lookup, default 'Sample_NAme'
  :param orig_sample_id: Column name for keeping original sample ids, default 'Original_Sample_ID'
  :param orig_sample_name: Column name for keeping original sample_names, default: 'Original_Sample_Name'
  :param orig_index: Column name for keeping original index, default 'Original_index'

  '''

  def __init__(
    self,samplesheet_file,singlecell_barcode_json,singlecell_tag='10X',
    index_column='index',sample_id_column='Sample_ID',sample_name_column='Sample_Name',
    orig_sample_id='Original_Sample_ID',orig_sample_name='Original_Sample_Name',
    sample_description_column='Description',orig_index='Original_index'):
    self.samplesheet_file = samplesheet_file
    self.singlecell_barcode_json = singlecell_barcode_json
    self.index_column = index_column
    self.sample_id_column = sample_id_column
    self.sample_name_column = sample_name_column
    self.sample_description_column = sample_description_column
    self.singlecell_barcodes = \
      ProcessSingleCellSamplesheet.\
        _get_index_data(singlecell_barcode_json)           # get single cell indexes
    self.singlecell_tag = singlecell_tag
    self.orig_sample_id = orig_sample_id
    self.orig_sample_name = orig_sample_name
    self.orig_index = orig_index


  @staticmethod
  def _get_index_data(singlecell_barcode_json):
    '''
    An internal static method for reading single cell index data json files

    :param singlecell_barcode_json: A JSON file containing single cell barcodes
    :returns: A dictionary
    '''
    try:
      check_file_path(singlecell_barcode_json)
      with open(singlecell_barcode_json) as json_data:
        index_json = json.load(json_data)

      index_data = dict()
      for index_line in index_json:
        index_name,index_list = index_line
        index_data[index_name] = index_list
      return index_data
    except:
      raise


  def _process_samplesheet_lines(self,data):
    '''
    An internal method for processing single cell indexes. Four lines of sample
    information are added to the output for each of the single cell samples

    :param data: A dictionary containing data of single line of samplesheet
    :returns: a list of samplesheet data
    '''
    try:
      if not isinstance(data,dict):
        raise ValueError('expecting a dictionary and got {0}'.format(type(data)))
      if data[self.sample_description_column]==self.singlecell_tag:
        if self.index_column in data:
          sc_index = data[self.index_column]
          index_data = self.singlecell_barcodes
          if sc_index in index_data:
            final_data = list()
            suffix = 0
            for index_seq in index_data[sc_index]:
              suffix += 1
              mod_data = dict(data)
              mod_data[self.orig_index] = \
                mod_data[self.index_column]
              mod_data[self.orig_sample_id] = \
                mod_data[self.sample_id_column]
              mod_data[self.orig_sample_name] = \
                mod_data[self.sample_name_column]                               # keep original sample infos
              mod_data[self.index_column] = index_seq                           # add sc index
              mod_data[self.sample_id_column] = \
                '{0}_{1}'.format(
                  mod_data[self.sample_id_column],
                  suffix)                                                       # add sc sample id
              mod_data[self.sample_name_column] = \
                '{0}_{1}'.format(
                  mod_data[self.sample_name_column],
                  suffix)                                                       # add sc sample name
              final_data.append(mod_data)
          else:
            raise ValueError('index {0} not found in file {1}'.\
                             format(sc_index,self.singlecell_barcode_json))
        else:
          raise ValueError('index column {0} not found in samplesheet data'.\
                           format(self.index_column))
        data = final_data
      else:
        data[self.orig_index] = ''
        data[self.orig_sample_id] = ''
        data[self.orig_sample_name] = ''
      return data
    except:
      raise


  def change_singlecell_barcodes(self,output_samplesheet):
    '''
    A method for replacing single cell index codes present in the samplesheet 
    with the four index sequences. This method will create 4 samplesheet entries
    for each of the single cell samples with _1 to _4 suffix and relevant indexes

    :param output_samplesheet: A file name of the output samplesheet
    '''
    try:
      samplesheet_sc = \
        SampleSheet(infile=self.samplesheet_file)
      new_samplesheet_data = list()
      if len(samplesheet_sc._data) > 0:                                         # single cell samples are present
        for data in samplesheet_sc._data:
          processed_data = \
            self._process_samplesheet_lines(data)
          if isinstance(processed_data,list):
            new_samplesheet_data.extend(processed_data)
          else:
            new_samplesheet_data.append(processed_data)
      samplesheet_sc.\
        _data_header.extend([
          self.orig_index,
          self.orig_sample_id,
          self.orig_sample_name])                                               # added new column names
      samplesheet_sc._data = \
        new_samplesheet_data                                                    # add modified single cell records
      samplesheet_sc.\
        print_sampleSheet(outfile=output_samplesheet)                           # write modified samplesheet
    except:
      raise