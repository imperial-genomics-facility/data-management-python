#!/usr/bin/env python
import os, subprocess
from shlex import quote
from shutil import copytree
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.tools.cellranger.cellranger_count_utils import get_cellranger_count_input_list
from igf_data.utils.tools.cellranger.cellranger_count_utils import check_cellranger_count_output
from igf_data.utils.tools.cellranger.cellranger_count_utils import get_cellranger_reference_genome

class RunellrangerCount(IGFBaseProcess):
  '''
  A ehive process class for running cellranger count pipeline
  '''
  def param_defaults(self):
    params_dict=super(RunBcl2Fastq,self).param_defaults()
    params_dict.update({
        'force_overwrite':True,
        'cellranger_exe':None,
        'fastq_collection_type':'demultiplexed_fastq',
        'reference_type':'cellranger_reference',
        'cellranger_options':'{ "--jobmode":"pbspro","--localcores":"1","--localmem":"4","--mempercore":"4","--maxjobs":"20"}',
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
    :param reference_type: Reference genome collection type, default cellranger_reference
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
      species_name=self.param('species_name')
      reference_type=self.param('reference_type')

      job_name=self.job_name()
      work_dir=os.path.join(base_work_dir,seqrun_igf_id,job_name)               # get work directory name
      if not os.path.exists(work_dir):
        os.makedirs(work_dir,mode=0o770)                                        # create work directory

      os.chdir(work_dir)                                                        # move to work dir
      reference_genome=get_cellranger_reference_genome(\
                         collection_name=species_name,
                         collection_type=reference_type,
                         session_class=igf_session_class)                       # fetch reference genome for cellranger run
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
                      '{0}={1}'.format('--transcriptome',quote(reference_genome)),
                     ]                                                          # set initial parameters
      cellranger_cmd.extend(cellranger_options)                                 # add optional parameters
      subprocess.check_call(cellranger_cmd)                                     # run cellranger count
      cellranger_output=os.path.join(work_dir,
                                     experiment_igf_id,
                                     'outs')                                    # get cellranger output path
      check_cellranger_count_output(output_path=cellranger_output)              # check output file
      self.param('dataflow_params',{'cellranger_output':cellranger_output})     # pass on cellranger output path
    except:
      message='project: {2}, sample:{3}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      project_igf_id,
                                                      sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise