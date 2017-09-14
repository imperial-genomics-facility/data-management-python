#!/usr/bin/env python
import os, subprocess
from ehive.runnable.IGFBaseProcess import IGFBaseProcess


class RunBcl2Fastq(IGFBaseProcess):
  '''
  A process class for running tool bcl2fastq
  '''
  def param_defaults(self):
    params_dict=IGFBaseProcess.param_defaults()
    params_dict.update({
        'runinfo_filename':'RunInfo.xml',
        'bcl2fastq_module':'bcl2fastq/2.18',
        'bcl2fastq_exe','bcl2fastq'
        'bcl2fastq_options':{'-r':1,'-w':1,'-p':1,'--barcode-mismatches':1, '--auto-set-to-zero-barcode-mismatches':True},
      })
    return params_dict
  
  def run(self):
    try:
      seqrun_igf_id=self.param_required('seqrun_igf_id')
      seqrun_local_dir=self.param_required('seqrun_local_dir')
      bases_mask=self.param_required('bases_mask')
      base_work_dir=self.param_required('base_work_dir')
      samplesheet_file=self.param_required('samplesheet')
      runinfo_filename=self.param('runinfo_filename')
      bcl2fastq_module=self.param('bcl2fastq_module')
      bcl2fastq_exe=self.param('bcl2fastq_exe')
      bcl2fastq_options=self.param('bcl2fastq_options')
      
      if not os.path.exists(samplesheet_file):
        raise IOError('samplesheet file {0} not found'.format(samplesheet_file))
      runinfo_file=os.path.join(seqrun_local_dir,seqrun_igf_id,runinfo_filename)
      
      if not os.path.exists(runinfo_file):
        raise IOError('Runinfo file {0} not found'.format(runinfo_file))
      
      subprocess.check_call(['module','load',bcl2fastq_module])                 # load module for bcl2fastq
      bcl2fastq_param=[[param,value] if value else [param] 
                       for param, value in bcl2fastq_options.items()]           # remove empty values
      bcl2fastq_param=[col for row in param for col in bcl2fastq_param]         # flatten list
    except:
      raise