#!/usr/bin/env python
import os, subprocess
from shlex import quote
from shutil import copytree
from fnmatch import fnmatch
from igf_data.utils.fileutils import get_temp_dir,remove_dir,move_file,check_file_path
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.tools.cellranger.cellranger_count_utils import get_cellranger_count_input_list
from igf_data.utils.tools.cellranger.cellranger_count_utils import check_cellranger_count_output
from igf_data.utils.tools.reference_genome_utils import Reference_genome_utils

class RunCellrangerCount(IGFBaseProcess):
  '''
  A ehive process class for running cellranger count pipeline
  '''
  def param_defaults(self):
    params_dict=super(RunCellrangerCount,self).param_defaults()
    params_dict.update({
        'force_overwrite':True,
        'cellranger_exe':None,
        'fastq_collection_type':'demultiplexed_fastq',
        'reference_type':'TRANSCRIPTOME_TENX',
        'nuclei_reference_type':'TRANSCRIPTOME_TENX_NUCLEI',
        'cellranger_options':'{"--nopreflight":"","--disable-ui":"", "--jobmode":"pbspro","--localcores":"1","--localmem":"4","--mempercore":"4","--maxjobs":"20"}',
        'job_timeout':43200,
        'manifest_filename':'file_manifest.csv',
        'analysis_name':'cellranger_count',
        'collection_type':'CELLRANGER_RESULTS',
        'collection_table':'experiment',
        'nuclei_biomaterial_type':'SINGLE_NUCLEI',
      })
    return params_dict

  def run(self):
    '''
    A method for running the cellranger count for a given sample using ehive pipeline
    
    :param project_igf_id: A project igf id
    :param experiment_igf_id: An experiment igf id
    :param sample_igf_id: A sample igf id
    :param biomaterial_type: Biomaterial type for samples, required for nuclei samples
    :param nuclei_biomaterial_type: Required keywords for nuclei samples, default 'SINGLE_NUCLEI'
    :param igf_session_class: A database session class
    :param cellranger_exe: Cellranger executable path
    :param cellranger_options: Cellranger parameters
                               
                               List of default parameters
                                 --jobmode=pbspro
                                 --localcores=1
                                 --localmem=4
                                 --mempercore=4
                                 --maxjobs=20
    
    :param base_work_dir: Base work directory path
    :param fastq_collection_type: Collection type name for input fastq files, default demultiplexed_fastq
    :param species_name: Reference genome collection name
    :param reference_type: Reference genome collection type, default TRANSCRIPTOME_TENX
    :param nuclei_reference_type: Reference genome collection type for pre-mRNA samples, default TRANSCRIPTOME_TENX_NUCLEI
    :param job_timeout: Timeout for cellranger job, default 24hrs
    :returns: Adding cellranger_output to the dataflow_params
    '''
    try:
      project_igf_id = self.param_required('project_igf_id')
      experiment_igf_id = self.param_required('experiment_igf_id')
      sample_igf_id = self.param_required('sample_igf_id')
      igf_session_class = self.param_required('igf_session_class')
      cellranger_exe = self.param_required('cellranger_exe')
      cellranger_options = self.param_required('cellranger_options')
      base_work_dir = self.param_required('base_work_dir')
      fastq_collection_type = self.param_required('fastq_collection_type')
      biomaterial_type = self.param_required('biomaterial_type')
      job_timeout = self.param_required('job_timeout')
      nuclei_biomaterial_type = self.param('nuclei_biomaterial_type')
      species_name = self.param('species_name')
      reference_type = self.param('reference_type')
      nuclei_reference_type = self.param('nuclei_reference_type')

      # setup work dir for run
      work_dir = False
      work_dir_prefix = \
        os.path.join(\
          base_work_dir,
          project_igf_id,
          sample_igf_id,
          experiment_igf_id)
      work_dir=self.get_job_work_dir(work_dir=work_dir_prefix)                  # replace this with temp dir while running in queue
      # setup env for run
      os.chdir(work_dir)                                                        # move to work dir
      os.environ['PATH'] += '{0}{1}'.format(os.pathsep,
                                            os.path.dirname(cellranger_exe))    # add cellranger location to env PATH
      # collect reference genome for run
      if biomaterial_type == nuclei_biomaterial_type:
        ref_genome = \
          Reference_genome_utils(\
            genome_tag=species_name,
            dbsession_class=igf_session_class,
            tenx_ref_type=nuclei_reference_type)                                # fetch ref genome for pre-mRNA samples
      else:
        ref_genome = \
          Reference_genome_utils(\
            genome_tag=species_name,
            dbsession_class=igf_session_class,
            tenx_ref_type=reference_type)

      # collect fastq input for run
      cellranger_ref_transcriptome = ref_genome.get_transcriptome_tenx()        # fetch tenx ref transcriptome from db
      input_fastq_dirs = \
        get_cellranger_count_input_list(\
          db_session_class=igf_session_class,
          experiment_igf_id=experiment_igf_id,
          fastq_collection_type=fastq_collection_type)                          # fetch fastq dir paths as list for run
      # configure cellranger count command for run
      cellranger_options = \
        self.format_tool_options(\
          cellranger_options,
          separator='=')
      cellranger_cmd = \
        [cellranger_exe,
         'count',
         '{0}={1}'.format('--fastqs',
                          quote(','.join(input_fastq_dirs))),
         '{0}={1}'.format('--id',
                          quote(experiment_igf_id)),
         '{0}={1}'.format('--transcriptome',
                          quote(cellranger_ref_transcriptome)),
        ]                                                                       # set initial parameters
      cellranger_cmd.extend(cellranger_options)                                 # add optional parameters
      # log before job submission
      message = \
        'started cellranger count for {0}, {1} {2}'.\
        format(\
          project_igf_id,
          sample_igf_id,
          experiment_igf_id)
      self.post_message_to_slack(message,reaction='pass')                       # send log to slack
      self.comment_asana_task(task_name=project_igf_id, comment=message)        # send comment to Asana
      self.post_message_to_ms_team(
          message=message,
          reaction='pass')
      message = ' '.join(cellranger_cmd)
      self.comment_asana_task(task_name=project_igf_id, comment=message)        # send cellranger command to Asana
      # start job execution
      cellranger_cmd = ' '.join(cellranger_cmd)                                 # create shell command string
      subprocess.\
        check_call(\
          cellranger_cmd,
          shell=True,
          timeout=job_timeout)                                                  # run cellranger count using shell
      # prepare output after cellranger run
      cellranger_output = \
        os.path.join(\
          work_dir,
          experiment_igf_id,
          'outs')                                                               # get cellranger output path
      message = \
        'finished cellranger count for {0}, {1} {2} : {3}'.\
        format(\
          project_igf_id,
          sample_igf_id,
          experiment_igf_id,
          cellranger_output)
      self.post_message_to_slack(message,reaction='pass')                       # send log to slack
      self.comment_asana_task(task_name=project_igf_id, comment=message)        # send comment to Asana
      self.post_message_to_ms_team(
          message=message,
          reaction='pass')
      # validate output files after cellranger run
      check_cellranger_count_output(output_path=cellranger_output)              # check output file
      cellranger_report = \
        os.path.join(\
          cellranger_output,
          'web_summary.html')
      check_file_path(cellranger_report)

      self.param('dataflow_params',\
                 {'cellranger_output':cellranger_output,
                  'cellranger_report':cellranger_report})                       # pass on cellranger output path
    except Exception as e:
      message = \
        'project: {2}, sample:{3}, Error in {0}: {1}'.\
        format(\
          self.__class__.__name__,
          e,
          project_igf_id,
          sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      self.post_message_to_ms_team(
          message=message,
          reaction='fail')
      if work_dir:
        remove_dir(work_dir)
      raise