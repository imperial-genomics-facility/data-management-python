#!/usr/bin/env python
import os, subprocess
from shlex import quote
from shutil import copytree
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.tools.cellranger.cellranger_count_utils import get_cellranger_count_input_list
from igf_data.utils.tools.cellranger.cellranger_count_utils import check_cellranger_count_outrput
from igf_data.utils.tools.cellranger.cellranger_count_utils import get_cellranger_reference_genome

class RunBcl2Fastq(IGFBaseProcess):
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
    try:
      project_igf_id=self.param_required('project_igf_id')
      experiment_igf_id=self.param_required('experiment_igf_id')
      sample_igf_id=self.param_required('sample_igf_id')
      sample_submitter_id=self.param_required('sample_submitter_id')
      igf_session_class=self.param_required('igf_session_class')
      cellranger_exe=self.param_required('cellranger_exe')
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
      check_cellranger_count_outrput(output_path=os.path.join(work_dir,
                                                              experiment_igf_id,
                                                              'outs'))          # check output file

    except:
      message='project: {2}, sample:{3}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      project_igf_id,
                                                      sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise