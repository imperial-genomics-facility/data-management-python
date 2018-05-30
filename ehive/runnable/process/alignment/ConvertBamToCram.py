#!/usr/bin/env python
import os
from igf_data.utils.tools.samtools_utils import convert_bam_to_cram
from ehive.runnable.IGFBaseProcess import IGFBaseProcess

class ConvertBamToCram(IGFBaseProcess):
  '''
  A ehive process class for converting bam files to cram files
  '''
  def param_defaults(self):
    params_dict=super(RunBcl2Fastq,self).param_defaults()
    params_dict.update({
        'force_overwrite':True,
        'reference_type':None,
      })
    return params_dict

  def run(self):
    '''
    A method for running bam to cram conversion
    
    :param project_igf_id: A project igf id
    :param experiment_igf_id: An experiment igf id
    :param sample_igf_id: A sample igf id
    :param igf_session_class: A database session class
    :param reference_type:  Reference genome collection type, should have suffix '_fasta'
    :param species_name: Reference genome collection name
    :param base_work_dir: Base work directory path
    '''
    try:
      project_igf_id=self.param_required('project_igf_id')
      experiment_igf_id=self.param_required('experiment_igf_id')
      sample_igf_id=self.param_required('sample_igf_id')
      sample_submitter_id=self.param_required('sample_submitter_id')
      igf_session_class=self.param_required('igf_session_class')
      reference_type=self.param_required('reference_type')
      species_name=self.param_required('species_name')
      bam_file=self.param_required('bam_file')
      base_work_dir=self.param_required('base_work_dir')

      work_dir=os.path.join(base_work_dir,project_igf_id,sample_igf_id,experiment_igf_id)
      work_dir=self.get_job_work_dir(work_dir,work_dir)
    except:
      raise