#!/usr/bin/env python
import os
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.illumina.runinfo_xml import RunInfo_xml
from igf_data.illumina.runparameters_xml import RunParameter_xml
from igf_data.igfdb.platformadaptor import PlatformAdaptor

class CheckAndProcessSampleSheet(IGFBaseProcess):
  '''
  A class for checking and processing samplesheet
  '''
  def param_defaults(self):
    return {
        'samplesheet_filename':'SampleSheet.csv',
        'index2_label':'index_2',
        'revcomp_label':'REVCOMP',
        '10X_label':'10X'
      }
  
  
  def run(self):
    try:
      igf_session_class = self.param_required('igf_session_class')
      seqrun_igf_id=self.param_required('seqrun_igf_id')
      seqrun_local_dir=self.param_required('seqrun_local_dir')
      base_work_dir=self.param_required('base_work_dir')
      samplesheet_filename=self.param('samplesheet_filename')
      index2_label=self.param('index2_label')
      revcomp_label=self.param('revcomp_label')
      tenX_label=self.param('10X_label')
      
      work_dir=os.path.join(base_work_dir,seqrun_igf_id,)
      samplesheet_file=os.path.join(seqrun_local_dir, seqrun_igf_id, samplesheet_filename)
      if not os.path.exists(samplesheet_file):
        raise IOError('sampleshhet file {0} not found'.format(samplesheet_file))
    
      samplesheet=SampleSheet(infile=samplesheet_file) # read samplesheet
      # separate 10X samplesheet
      samplesheet.filter_sample_data(condition_key='Description', condition_value=tenX_label, method='exclude')
      
      # convert index based on barcode rules
      sa=SeqrunAdaptor(**{'session_class':igf_session_class})
      sa.start_session()
      rules_data=sa.fetch_flowcell_barcode_rules_for_seqrun(seqrun_igf_id)
      sa.close_session()
      
      rules_data=rules_data.to_dict(orient='records')[0]  # convert dataframe to dictionary
      
      if rules_data[index2_label]==revcomp_label:
        samplesheet.get_reverse_complement_index(index_field='index2')
      
      
      # no need to add revcomp method for index1
      
      # 
      
      
    except:
      raise