import os
from igf_data.illumina.samplesheet import SampleSheet
class MergeSingleCellFastq:
  '''
  A class for merging single cell fastq files per lane per sample
  
  required params:
  fastq_dir: A directory path containing fastq files
  samplesheet: A samplesheet file used demultiplexing of bcl files
  platform_name: A sequencing platform name
  singlecell_tag: A single cell keyword for description field, default '10X'
  sampleid_col: A keyword for sample id column of samplesheet, default 'Sample_ID'
  samplename_col: A keyword for sample name column of samplesheet, default 'Sample_Name'
  orig_sampleid_col: A keyword for original sample id column, default 'Original_Sample_ID'
  orig_samplename_col: A keyword for original sample name column, default 'Original_Sample_Name'
  description_col: A keyword for description column, default 'Description'
  
  SampleSheet file should contain following columns:
  Sample_ID: A single cell sample id in the following format, SampleId_{digit}
  Sample_Name: A single cell sample name in the following format, SampleName_{digit}
  Original_Sample_ID: An IGF sample id
  Original_Sample_Name: A sample name provided by user
  Description: A single cell label, default 10X
  '''
  def __init__(self, fastq_dir,samplesheet,platform_name,singlecell_tag='10X', 
               sampleid_col='Sample_ID', samplename_col='Sample_Name', 
               orig_sampleid_col='Original_Sample_ID', description_col='Description', 
               orig_samplename_col='Original_Sample_Name'):
    self.fastq_dir=fastq_dir
    self.samplesheet=samplesheet
    self.platform_name=platform_name
    self.singlecell_tag=singlecell_tag
    self.sampleid_col=sampleid_col
    self.samplename_col=samplename_col
    self.orig_sampleid_col=orig_sampleid_col
    self.description_col=description_col
    self.orig_samplename_col=orig_samplename_col

  def _fetch_lane_and_sample_info_from_samplesheet(self):
    '''
    A internal method for grouping samples per lane based on the samplesheet
    returns a list containing sample and lane information per row
    '''
    try:
      samplesheet_data=SampleSheet(infile=self.samplesheet)                     # read samplesheet file
      if (self.orig_sampleid_col not in samplesheet_data._data_header) or \
         (self.orig_samplename_col not in samplesheet_data._data_header):
        raise ValueError('Samplesheet {0} does not have {1} or {2} column'.\
                         format(self.samplesheet,
                                self.orig_sampleid_col,
                                self.orig_samplename_col))                      # check for required columns in the samplesheet
      samplesheet_data.\
      filter_sample_data(condition_key=self.description_col,
                         condition_value=self.singlecell_tag,
                         method='include')                                      # filter samplesheet for single cell data
      sample_lane_data=list()
      if platform_name=='NEXTSEQ':                                              # hack for nextseq
        samplesheet_data.add_pseudo_lane_for_nextseq()
        for group_tag,_ in pd.DataFrame(samplesheet_data._data).\
                              groupby(['PseudoLane',
                                       self.orig_sampleid_col,
                                       self.orig_samplename_col]):
          sample_data.append({'lane_id':group_tag[0],
                              'sample_id':group_tag[1],
                              'sample_name':group_tag[2]})
      elif platform_name=='MISEQ':                                              # hack for miseq
        samplesheet_data.add_pseudo_lane_for_miseq()
        for group_tag,_ in pd.DataFrame(samplesheet_data._data).\
                              groupby(['PseudoLane',
                                       self.orig_sampleid_col,
                                       self.orig_samplename_col]):
          sample_data.append({'lane_id':group_tag[0],
                              'sample_id':group_tag[1],
                              'sample_name':group_tag[2]})
      elif platform_name=='HISEQ4000':                                          # check for hiseq4k
        for group_tag,_ in pd.DataFrame(samplesheet_data._data).\
                              groupby(['Lane',
                                       self.orig_sampleid_col,
                                       self.orig_samplename_col]):
          sample_data.append({'lane_id':group_tag[0],
                              'sample_id':group_tag[1],
                              'sample_name':group_tag[2]})
      else:
        raise ValueError('platform {0} not supported'.format(platform_name))
      return sample_data
    except:
      raise

  def merge_fastq_per_lane_per_sample(self,output_dir):
    '''
    A method for merging single cell fastq files present in input fastq_dir
    per lane per sample basis
    
    required params:
    output_dir: A directory path for writing output fastq files
    '''
    try:
      sample_data=self._fetch_lane_and_sample_info_from_samplesheet()           # get sample and lane information from samplesheet
    except:
      raise