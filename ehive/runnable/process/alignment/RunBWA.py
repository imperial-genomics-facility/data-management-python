import os
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.tools.bwa_utils import BWA_util
from igf_data.utils.tools.reference_genome_utils import Reference_genome_utils

class RunBWA(IGFBaseProcess):
  def param_defaults(self):
    params_dict=super(RunBWA,self).param_defaults()
    params_dict.update({
        'reference_type':'GENOME_BWA',
        'run_thread':1,
        'r2_read_file':None
      })
    return params_dict

  def run(self):
    '''
    A method for running BWA alignment
    
    '''
    try:
      project_igf_id=self.param_required('project_igf_id')
      experiment_igf_id=self.param_required('experiment_igf_id')
      sample_igf_id=self.param_required('sample_igf_id')
      run_igf_id=self.param_required('run_igf_id')
      bwa_exe=self.param_required('bwa_exe')
      samtools_exe=self.param_required('samtools_exe')
      r1_read_file=self.param_required('r1_read_file')
      r2_read_file=self.param('r2_read_file')
      run_thread=self.param('run_thread')
      output_prefix=self.param_required('output_prefix')
      igf_session_class=self.param_required('igf_session_class')
      species_name=self.param('species_name')
      reference_type=self.param('reference_type')
      input_fastq_list=list()
      input_fastq_list.append(r1_read_file)
      if r2_read_file is not None:
        input_fastq_list.append(r2_read_file)

      work_dir_prefix=os.path.join(base_work_dir,
                                   project_igf_id,
                                   sample_igf_id,
                                   experiment_igf_id,
                                   run_igf_id)
      work_dir=self.get_job_work_dir(work_dir=work_dir_prefix)                  # get a run work dir
      ref_genome=Reference_genome_utils(\
                   genome_tag=species_name,
                   dbsession_class=igf_session_class,
                   bwa_ref_type=reference_type)                                 # setup ref genome utils
      bwa_ref=ref_genome.get_genome_bwa()                                       # get bwa ref
      bwa_obj=BWA_util(bwa_exe=bwa_exe,
                       samtools_exe=samtools_exe,
                       ref_genome=bwa_ref,
                       input_fastq_list=input_fastq_list,
                       output_dir=work_dir,
                       output_prefix=output_prefix,
                       bam_output=True,
                       thread=run_thread)                                       # set up bwa for run
      final_output_file,bwa_cmd=BWA_util.run_mem()                              # run bwa mem
      self.param('dataflow_params',{'bwa_bam':final_output_file,
                                    'output_prefix':output_prefix
                                   })                                           # pass on bwa output list
      message='finished bwa {0} for {1} {2} {3}'.\
              format(bwa_cmd,
                     project_igf_id,
                     run_igf_id,
                     output_prefix)
      self.post_message_to_slack(message,reaction='pass')                       # send log to slack
      self.comment_asana_task(task_name=project_igf_id, comment=message)        # send comment to Asana
      message='Bwa {0} {1} command: {2}'.\
              format(run_igf_id,
                     output_prefix,
                     bwa_cmd)
      self.comment_asana_task(task_name=project_igf_id, comment=message)        # send commandline to Asana
    except Exception as e:
      message='project: {2}, sample:{3}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      project_igf_id,
                                                      sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise