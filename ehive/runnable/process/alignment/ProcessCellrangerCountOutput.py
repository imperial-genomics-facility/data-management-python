import os
from fnmatch import fnmatch
from igf_data.utils.fileutils import get_temp_dir,remove_dir,move_file
from igf_data.utils.fileutils import create_file_manifest_for_dir
from igf_data.utils.fileutils import prepare_file_archive
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.tools.reference_genome_utils import Reference_genome_utils
from igf_data.utils.analysis_collection_utils import Analysis_collection_utils

class ProcessCellrangerCountOutput(IGFBaseProcess):
  '''
  A ehive process class for processing cellranger count output files
  '''
  def param_defaults(self):
    params_dict=super(ProcessCellrangerCountOutput,self).param_defaults()
    params_dict.update({
        'force_overwrite':True,
        'reference_type':'TRANSCRIPTOME_TENX',
        'manifest_filename':'file_manifest.csv',
        'analysis_name':'cellranger_count',
        'collection_type':'CELLRANGER_RESULTS',
        'collection_table':'experiment',
      })
    return params_dict

  def run(self):
    '''
    A method for running the cellranger count output processing for a given sample using ehive pipeline
    
    :param project_igf_id: A project igf id
    :param experiment_igf_id: An experiment igf id
    :param sample_igf_id: A sample igf id
    :param igf_session_class: A database session class
    :param cellranger_output: Cellranger output path
    :param base_work_dir: Base work directory path
    :param fastq_collection_type: Collection type name for input fastq files, default demultiplexed_fastq
    :param species_name: Reference genome collection name
    :param reference_type: Reference genome collection type, default TRANSCRIPTOME_TENX
    :returns: Adding cellranger_output to the dataflow_params
    '''
    try:
      project_igf_id=self.param_required('project_igf_id')
      experiment_igf_id=self.param_required('experiment_igf_id')
      sample_igf_id=self.param_required('sample_igf_id')
      sample_submitter_id=self.param_required('sample_submitter_id')
      igf_session_class=self.param_required('igf_session_class')
      cellranger_output=self.param_required('cellranger_output')
      base_work_dir=self.param_required('base_work_dir')
      base_result_dir=self.param_required('base_results_dir')
      species_name=self.param('species_name')
      reference_type=self.param('reference_type')
      manifest_filename=self.param('manifest_filename')
      analysis_name=self.param('analysis_name')
      collection_type=self.param('collection_type')
      collection_table=self.param('collection_table')

      # prepare manifest file for the results dir
      manifest_file=os.path.join(cellranger_output,
                                 manifest_filename)                             # get name of the manifest file
      create_file_manifest_for_dir(results_dirpath=cellranger_output,
                                   output_file=manifest_file,
                                   md5_label='md5',
                                   exclude_list=['*.bam','*.bai','*.cram'])     # create manifest for output dir
      # create archive for the results dir
      temp_archive_name=os.path.join(get_temp_dir(),
                                     '{0}.tar.gz'.format(experiment_igf_id))    # get the name of temp archive file
      prepare_file_archive(results_dirpath=cellranger_output,
                           output_file=temp_archive_name,
                           exclude_list=['*.bam','*.bai','*.cram'])             # archive cellranget output
      # load archive file to db collection and results dir
      au=Analysis_collection_utils(dbsession_class=igf_session_class,
                                   analysis_name=analysis_name,
                                   tag_name=species_name,
                                   collection_name=experiment_igf_id,
                                   collection_type=collection_type,
                                   collection_table=collection_table,
                                   base_path=base_result_dir)                   # initiate loading of archive file
      output_file_list=au.load_file_to_disk_and_db(\
                            input_file_list=[temp_archive_name],
                            withdraw_exisitng_collection=True)                  # load file to db and disk
      # find bam path for the data flow
      bam_list=list()                                                           # define empty bamfile list
      for file in os.listdir(cellranger_output):
        if fnmatch(file, '*.bam'):
          bam_list.append(os.path.join(cellranger_output,
                                       file))                                   # add all bams to bam_list

      if len(bam_list)>1:
        raise ValueError('More than one bam found for cellranger count run:{0}'.\
                         format(cellranger_output))                             # check number of bams, presence of one bam is already validated by check method

      bam_file=bam_list[0]
      au=Analysis_collection_utils(dbsession_class=igf_session_class,
                                   analysis_name=analysis_name,
                                   tag_name=species_name,
                                   collection_name=experiment_igf_id,
                                   collection_type=collection_type,
                                   collection_table=collection_table)           # initiate bam file rename
      new_bam_name=au.get_new_file_name(input_file=bam_file)
      if os.path.basename(bam_file)!=new_bam_name:
        new_bam_name=os.path.join(os.path.dirname(bam_file),
                              new_bam_name)                                     # get ne bam path
        move_file(source_path=bam_file,
                  destinationa_path=new_bam_name,
                  force=True)                                                   # move bam file

      self.param('dataflow_params',{'cellranger_output':cellranger_output,
                                    'bam_file':bam_file,
                                    'analysis_output_list':output_file_list
                                   })                                           # pass on cellranger output path
    except Exception as e:
      message='project: {2}, sample:{3}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      project_igf_id,
                                                      sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      if work_dir:
        remove_dir(work_dir)
      raise