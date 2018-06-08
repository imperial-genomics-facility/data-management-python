#!/usr/bin/env python
import os
from igf_data.utils.tools.samtools_utils import convert_bam_to_cram
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.analysis_collection_utils import Analysis_collection_utils
from igf_data.utils.tools.reference_genome_utils import Reference_genome_utils
from igf_data.utils.fileutils import get_temp_dir,remove_dir

class ConvertBamToCram(IGFBaseProcess):
  '''
  A ehive process class for converting bam files to cram files
  '''
  def param_defaults(self):
    params_dict=super(RunBcl2Fastq,self).param_defaults()
    params_dict.update({
        'force_overwrite':True,
        'reference_type':None,
        'collection_name':None,
        'analysis_name':None,
        'tag_name':None,
        'collection_type':'ANALYSIS_CRAM',
        'collection_table':'experiment',
      })
    return params_dict

  def run(self):
    '''
    A method for running bam to cram conversion
    
    :param project_igf_id: A project igf id
    :param sample_igf_id: A sample igf id
    :param igf_session_class: A database session class
    :param species_name: Reference genome collection name
    :param analysis_name: Name of the analysis for source bam
    :param tag_name: Additional tag for renaming file
    :param base_result_dir: Base work directory path
    :param collection_name: A database collection name for output file, default None
    :param collection_type: A database collection type for output file, default ANALYSIS_CRAM
    :param collection_table: A database collection table for output file, default experiment
    :param force_overwrite: A toggle for retiring old collection group, default True
    '''
    try:
      project_igf_id=self.param_required('project_igf_id')
      sample_igf_id=self.param_required('sample_igf_id')
      igf_session_class=self.param_required('igf_session_class')
      species_name=self.param_required('species_name')
      bam_file=self.param_required('bam_file')
      base_result_dir=self.param_required('base_result_dir')
      collection_name=self.param_required('collection_name')
      collection_type=self.param_required('collection_type')
      collection_table=self.param_required('collection_table')
      analysis_name=self.param_required('analysis_name')
      tag_name=self.param_required('tag_name')
      force_overwrite=self.param_required('force_overwrite')

      if collection_type is None or \
         collection_name is None or \
         collection_table is None:
        raise ValueError('Missing collection information for loading cram file')

      ref_genome=Reference_genome_utils(genome_tag=species_name,
                                        dbsession_class=igf_session_class)
      genome_fasta=ref_genome.get_genome_fasta()                                # get genome fasta 
      temp_work_dir=get_temp_dir()                                              # get temp dir
      cram_file=os.path.basename(bam_file).replace('.bam','.cram')              # get base cram file name
      cram_file=os.path.join(temp_work_dir,cram_file)                           # get cram file path in work dir
      convert_bam_to_cram(bam_file=bam_file,
                          reference_file=genome_fasta,
                          cram_path=cram_file)                                  # create new cramfile
      au=Analysis_collection_utils(dbsession_class=igf_session_class,
                                   analysis_name=analysis_name,
                                   tag_name=tag_name,
                                   collection_name=collection_name,
                                   collection_type=collection_type,
                                   collection_table=collection_table,
                                   base_path=base_result_dir)
      au.load_file_to_disk_and_db(input_file_list=[cram_file],
                                  withdraw_exisitng_collection=force_overwrite) # load file to db and disk
      self.param('dataflow_params',{'bam_file':bam_file})                       # pass on bam output path
    except Exception as e:
      message='project: {2}, sample:{3}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      project_igf_id,
                                                      sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise