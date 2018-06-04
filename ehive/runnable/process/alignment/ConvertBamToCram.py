#!/usr/bin/env python
import os
from igf_data.utils.tools.samtools_utils import convert_bam_to_cram
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.tools.cellranger.cellranger_count_utils import get_cellranger_reference_genome

class ConvertBamToCram(IGFBaseProcess):
  '''
  A ehive process class for converting bam files to cram files
  '''
  def param_defaults(self):
    params_dict=super(RunBcl2Fastq,self).param_defaults()
    params_dict.update({
        'force_overwrite':True,
        'reference_type':None,
        'load_file':True,
        'path_map':None,
        'collection_name':None,
        'collection_type':'ANALYSIS_CRAM',
        'collection_table':'experiment',
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
    :param load_file: A toggle for loading file to disk and database collection, default True
    :param base_result_dir: Base work directory path
    :param path_map: A path for loading output file in the base_result_dir, default None
    :param collection_name: A database collection name for output file, default None
    :param collection_type: A database collection type for output file, default ANALYSIS_CRAM
    :param collection_table: A database collection table for output file, default experiment
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
      base_result_dir=self.param_required('base_result_dir')
      load_file=self.param_required('load_file')
      path_map=self.param_required('path_map')
      collection_name=self.param_required('collection_name')
      collection_type=self.param_required('collection_type')
      collection_table=self.param_required('collection_table')

      reference_genome=get_cellranger_reference_genome(\
                         collection_name=species_name,
                         collection_type=reference_type,
                         session_class=igf_session_class)                       # fetch reference genome for cellranger run
      work_dir=os.path.join(base_work_dir,
                            project_igf_id,
                            sample_igf_id,
                            experiment_igf_id)
      work_dir=self.get_job_work_dir(work_dir,work_dir)                         # get workdir
      cram_file=os.path.basename(bam_file).replace('.bam','.cram')              # get base cram file name
      cram_file=os.path.join(work_dir,cram_file)                                # get cram file path in work dir
      convert_bam_to_cram(bam_file=bam_file,
                          reference_file=reference_genome,
                          cram_path=cram_file)                                  # create new cramfile
      self.param('dataflow_params',{'cram_file':cram_file})                     # pass on cram output path
    except:
      raise