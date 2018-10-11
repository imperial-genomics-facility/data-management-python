import os
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.tools.fastp_utils import Fastp_utils

class RunFastp(IGFBaseProcess):
  def param_defaults(self):
    params_dict=super(RunFastp,self).param_defaults()
    params_dict.update({
        'fastp_options_list':['--qualified_quality_phred 15',
                              '--length_required 15'],
        'split_by_lines_count':5000000,
        'run_thread':1,
        'split_fastq':False
      })
    return params_dict

  def run(self):
    '''
    A method for running Fastp commands
    
    '''
    try:
      project_igf_id=self.param_required('project_igf_id')
      experiment_igf_id=self.param_required('experiment_igf_id')
      sample_igf_id=self.param_required('sample_igf_id')
      fastp_exe=self.param_required('fastp_exe')
      input_fastq_list=self.param_required('input_fastq_list')
      base_work_dir=self.param_required('base_work_dir')
      run_thread=self.param('run_thread')
      split_fastq=self.param('split_fastq')
      split_by_lines_count=self.param('split_by_lines_count')
      fastp_options_list=self.param('fastp_options_list')
      work_dir_prefix=os.path.join(base_work_dir,
                                   project_igf_id,
                                   sample_igf_id,
                                   experiment_igf_id)
      work_dir=self.get_job_work_dir(work_dir=work_dir_prefix)                  # get a run work dir
      if split_fastq is None:
        split_fastq=False                                                       # set default value for split fastq

      fastp_obj=Fastp_utils(fastp_exe=fastp_exe,
                            input_fastq_list=input_fastq_list,
                            output_dir=work_dir,
                            run_thread=run_thread,
                            split_by_lines_count=split_by_lines_count,
                            fastp_options_list=fastp_options_list)              # setup fastp tool for run
      output_read1,output_read2,output_html_file,fastp_cmd=\
          fastp_obj.run_adapter_trimming(split_fastq=split_fastq)               # run fastp trimming
      self.param('dataflow_params',{'output_read1':output_read1,
                                    'output_read2':output_read2,
                                    'output_html_file':output_html_file
                                   })                                           # pass on fastp output list
      message='finished fastp {0} for {1} {2}'.\
              format(fastp_cmd,
                     project_igf_id,
                     sample_igf_id)
      self.post_message_to_slack(message,reaction='pass')                       # send log to slack
      self.comment_asana_task(task_name=project_igf_id, comment=message)        # send comment to Asana
      message='Fastp {0} command: {1}'.\
              format(experiment_igf_id,
                     fastp_cmd)
      self.comment_asana_task(task_name=project_igf_id, comment=message)        # send commandline to Asana
    except Exception as e:
      message='project: {2}, sample:{3}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      project_igf_id,
                                                      sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise