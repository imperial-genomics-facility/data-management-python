#!/usr/bun/env python
import os
from igf_data.illumina.basesMask import BasesMask
from ehive.runnable.IGFBaseProcess import IGFBaseProcess



class CalculateBasesMask(IGFBaseProcess):
  '''
  A process classs for calculation of bases mask value based on
  Illumina samplesheet and RunInfor.xml file
  '''
  def param_defaults(self):
    params_dict=IGFBaseProcess.param_defaults()
    params_dict.update({
        'samplesheet_filename':'SampleSheet.csv',
        'runinfo_filename':'RunInfo.xml',
        'read_offset':1,
        'index_offset':0,
      })
    return params_dict
  
  
  def run(self):
    try:
      seqrun_igf_id=self.param_required('seqrun_igf_id')
      seqrun_local_dir=self.param_required('seqrun_local_dir')
      samplesheet_file=self.param_required('samplesheet')
      samplesheet_filename=self.param('samplesheet_filename')
      runinfo_filename=self.aparam('runinfo_filename')
      read_offset=self.param('read_offset')
      index_offset=self.param('index_offset')
      runinfo_file=os.path.join(seqrun_local_dir,seqrun_igf_id,runinfo_filename)
    
      if not os.path.exists(samplesheet_file):
        raise IOError('samplesheet file {0} not found'.format(samplesheet_file))
    
      if not os.path.exists(runinfo_file):
        raise IOError('RunInfo file {0} file not found'.format(runinfo_file))
    
      bases_mask_object=BasesMask(samplesheet_file=samplesheet_file,
                                  runinfo_file=runinfo_file,
                                  read_offset=read_offset,
                                  index_offset=index_offset)                      # create bases mask object
      bases_mask_value=bases_mask_object.calculate_bases_mask()                   # calculate bases mask
      self.param('dataflow_params',{'basesmask':bases_mask_value})
    except:
      raise