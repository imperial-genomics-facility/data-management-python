import os
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.tools.fastp_utils import Fastp_utils
from igf_data.utils.fileutils import get_datestamp_label

class RunFastp(IGFBaseProcess):
  def param_defaults(self):
    params_dict=super(RunFastp,self).param_defaults()
    params_dict.update({
        'fastp_options_list':['-a','auto',
                              '--qualified_quality_phred=15',
                              '--length_required=15'],
        'split_by_lines_count':5000000,
        'run_thread':1,
        'split_fastq':None,
        'polyg_platform_list':['NextSeq','NOVASEQ6000'],
        'enable_polyg_trim':False
      })
    return params_dict

  def run(self):
    '''
    A method for running Fastp commands
    
    :param project_igf_id: A project_igf_id from dataflow
    :param experiment_igf_id: A experiment_igf_id from dataflow
    :param sample_igf_id: A sample_igf_id from dataflow
    :param fastp_exe: Fastp exe path from analysis config
    :param input_fastq_list: Input fastq list from dataflow
    :param base_work_dir: Base work dir path from analysis config
    :param run_thread: Number of threads for fastp run, default 1
    :param split_fastq: Enable splitting fastq files, default None
    :param split_by_lines_count: Number of fastq lines to be used if split_fastq is True, default 5000000
    :param fastp_options_list: A list of fasrp tool options, default ['-a=auto','--qualified_quality_phred=15','--length_required=15']
    :param platform_name: Sequencing platform name from dataflow
    :param polyg_platform_list: A list of Illumin platforms which emit poly Gs for empty cycles, default ['NextSeq','NOVASEQ6000']
    :param enable_polyg_trim: Enable Fastp poly G trim, default False
    '''
    try:
      project_igf_id = self.param_required('project_igf_id')
      experiment_igf_id = self.param_required('experiment_igf_id')
      sample_igf_id = self.param_required('sample_igf_id')
      run_igf_id = self.param_required('run_igf_id')
      fastp_exe = self.param_required('fastp_exe')
      input_fastq_list = self.param_required('input_fastq_list')
      base_work_dir = self.param_required('base_work_dir')
      run_thread = self.param('run_thread')
      split_fastq = self.param('split_fastq')
      split_by_lines_count = self.param('split_by_lines_count')
      fastp_options_list = self.param('fastp_options_list')
      platform_name = self.param_required('platform_name')
      polyg_platform_list = self.param('polyg_platform_list')
      enable_polyg_trim = self.param('enable_polyg_trim')
      seed_date_stamp = self.param_required('date_stamp')
      seed_date_stamp = get_datestamp_label(seed_date_stamp)
      work_dir_prefix = \
        os.path.join(\
          base_work_dir,
          project_igf_id,
          sample_igf_id,
          experiment_igf_id)
      work_dir = self.get_job_work_dir(work_dir=work_dir_prefix)                # get a run work dir
      split_fastq = \
        False if split_fastq is None else True                                  # set default value for split fastq
      if platform_name in polyg_platform_list:
        enable_polyg_trim=True                                                  # enable poly G trim for new Illumin platforms

      fastp_obj = \
        Fastp_utils(\
          fastp_exe=fastp_exe,
          input_fastq_list=input_fastq_list,
          log_output_prefix=run_igf_id,
          output_dir=work_dir,
          run_thread=run_thread,
          enable_polyg_trim=enable_polyg_trim,
          split_by_lines_count=split_by_lines_count,
          fastp_options_list=fastp_options_list)                                # setup fastp tool for run
      output_read1, output_read2, output_html_file, output_json_file, _ = \
        fastp_obj.\
          run_adapter_trimming(split_fastq=split_fastq)                         # run fastp trimming
      self.param('dataflow_params',
                 {'output_read1':output_read1,
                  'output_read2':output_read2,
                  'output_html_file':output_html_file,
                  'output_json_file':output_json_file,
                  'seed_date_stamp':seed_date_stamp})                           # pass on fastp output list
      message = 'finished fastp for {0} {1}'.\
                format(project_igf_id,
                       sample_igf_id)
      self.post_message_to_slack(message,reaction='pass')                       # send log to slack
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
      raise