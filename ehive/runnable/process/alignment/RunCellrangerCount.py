#!/usr/bin/env python
import os, subprocess
from shlex import quote
from shutil import copytree
from fnmatch import fnmatch
from igf_data.utils.fileutils import get_temp_dir,remove_dir
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
        'cellranger_options':'{ "--jobmode":"pbspro","--localcores":"1","--localmem":"4","--mempercore":"4","--maxjobs":"20"}',
        'job_timeout':86400,
      })
    return params_dict

  def run(self):
    '''
    A method for running the cellranger count for a given sample using ehive pipeline
    
    :param project_igf_id: A project igf id
    :param experiment_igf_id: An experiment igf id
    :param sample_igf_id: A sample igf id
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
    :param job_timeout: Timeout for cellranger job, default 24hrs
    :returns: Adding cellranger_output to the dataflow_params
    '''
    try:
      project_igf_id=self.param_required('project_igf_id')
      experiment_igf_id=self.param_required('experiment_igf_id')
      sample_igf_id=self.param_required('sample_igf_id')
      sample_submitter_id=self.param_required('sample_submitter_id')
      igf_session_class=self.param_required('igf_session_class')
      cellranger_exe=self.param_required('cellranger_exe')
      cellranger_options=self.param_required('cellranger_options')
      base_work_dir=self.param_required('base_work_dir')
      fastq_collection_type=self.param_required('fastq_collection_type')
      job_timeout=self.param_required('job_timeout')
      species_name=self.param('species_name')
      reference_type=self.param('reference_type')

      work_dir=os.path.join(base_work_dir,project_igf_id,sample_igf_id,experiment_igf_id)
      work_dir=self.get_job_work_dir(work_dir=work_dir)

      os.chdir(work_dir)                                                        # move to work dir
      os.environ['PATH'] += '{0}{1}'.format(os.pathsep,
                                            os.path.basename(cellranger_exe))   # add cellranger path to env
      ref_genome=Reference_genome_utils(genome_tag=species_name,
                                        dbsession_class=igf_session_class,
                                        tenx_ref_type=reference_type)
      cellranger_ref_transcriptome=ref_genome.get_transcriptome_tenx()          # fetch tenx ref transcriptome from db
      input_fastq_dirs=get_cellranger_count_input_list(\
                         db_session_class=igf_session_class,
                         experiment_igf_id=experiment_igf_id,
                         fastq_collection_type=fastq_collection_type)           # fetch fastq dir paths as list for run
      cellranger_options=self.format_tool_options(cellranger_options,
                                                  separator='=')
      cellranger_cmd=[cellranger_exe,
                      'count'
                      '{0}={1}'.format('--fastqs=',
                                       quote(','.join(input_fastq_dirs))),
                      '{0}={1}'.format('--id=',quote(experiment_igf_id)),
                      '{0}={1}'.format('--sample=',quote(sample_submitter_id)),
                      '{0}={1}'.format('--transcriptome',quote(cellranger_ref_transcriptome)),
                     ]                                                          # set initial parameters
      cellranger_cmd.extend(cellranger_options)                                 # add optional parameters
      message='started cellranger count for {0}, {1} {2}'.\
              format(project_igf_id,
                     sample_igf_id,
                     experiment_igf_id)
      self.post_message_to_slack(message,reaction='pass')                       # send log to slack
      self.comment_asana_task(task_name=project_igf_id, comment=message)        # send comment to Asana
      message=' '.join(cellranger_cmd)
      self.comment_asana_task(task_name=project_igf_id, comment=message)        # send cellranger command to Asana

      subprocess.check_call(cellranger_cmd,
                            timeout=job_timeout)                                # run cellranger count
      cellranger_output=os.path.join(work_dir,
                                     experiment_igf_id,
                                     'outs')                                    # get cellranger output path
      message='finished cellranger count for {0}, {1} {2} : {3}'.\
              format(project_igf_id,
                     sample_igf_id,
                     experiment_igf_id,
                     cellranger_output)
      self.post_message_to_slack(message,reaction='pass')                       # send log to slack
      self.comment_asana_task(task_name=project_igf_id, comment=message)        # send comment to Asana
      check_cellranger_count_output(output_path=cellranger_output)              # check output file
      bam_list=list()                                                           # define empty bamfile list
      for file in os.listdir(check_cellranger_count_output):
        if fnmatch(file, '*.bam'):
          bam_list.append(file)                                                 # add all bams to bam_list

      if len(bam_list)>1:
        raise ValueError('More than one bam found for cellranger count run:{0}'.\
                         format(check_cellranger_count_output))                 # check number of bams, presence of one bam is already validated by check method

      reference_type=reference_type.replace('transcriptome','fasta')            # changing reference type for cram conversion
      self.param('dataflow_params',{'cellranger_output':cellranger_output,
                                    'bam_file':bam_list[0],
                                    'reference_type':reference_type})           # pass on cellranger output path
    except Exception as e:
      message='project: {2}, sample:{3}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      project_igf_id,
                                                      sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise