import os
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.tools.rsem_utils import RSEM_utils
from igf_data.utils.fileutils import get_datestamp_label
from igf_data.utils.tools.reference_genome_utils import Reference_genome_utils

class RunRSEM(IGFBaseProcess):
  def param_defaults(self):
    params_dict=super(RunRSEM,self).param_defaults()
    params_dict.update({
      'reference_type':'TRANSCRIPTOME_RSEM',
      'strandedness':'reverse',
      'threads':1,
      'memory_limit':4000,
      'rsem_options':None,
      'force_overwrite':True,
      'rsem_options':None,
      'use_ephemeral_space':0,
    })
    return params_dict

  def run(self):
    '''
    '''
    try:
      project_igf_id = self.param_required('project_igf_id')
      experiment_igf_id = self.param_required('experiment_igf_id')
      sample_igf_id = self.param_required('sample_igf_id')
      rsem_exe_dir = self.param_required('rsem_exe_dir')
      library_layout = self.param_required('library_layout')
      reference_type = self.param_required('reference_type')
      igf_session_class = self.param_required('igf_session_class')
      output_prefix = self.param_required('output_prefix')
      base_work_dir = self.param_required('base_work_dir')
      input_bams = self.param_required('input_bams')
      strandedness = self.param('strandedness')
      threads = self.param('threads')
      use_ephemeral_space = self.param('use_ephemeral_space')
      memory_limit = self.param('memory_limit')
      rsem_options = self.param('rsem_options')
      force_overwrite = self.param('force_overwrite')
      species_name = self.param('species_name')
      base_work_dir = self.param_required('base_work_dir')
      seed_date_stamp = self.param_required('date_stamp')
      seed_date_stamp = get_datestamp_label(seed_date_stamp)
      if not isinstance(input_bams,list) or \
         len(input_bams) != 1:
        raise ValueError('Expecting one input bam for rsem and got : {0}'.\
                         format(len(input_bams)))

      work_dir_prefix = \
        os.path.join(
          base_work_dir,
          project_igf_id,
          sample_igf_id,
          experiment_igf_id)
      work_dir = \
        self.get_job_work_dir(work_dir=work_dir_prefix)                         # get a run work dir
      ref_genome = \
        Reference_genome_utils(
          genome_tag=species_name,
          dbsession_class=igf_session_class,
          gene_rsem_type=reference_type)
      rsem_ref = ref_genome.get_transcriptome_rsem()                            # fetch rsem refrence
      if library_layout =='PAIRED':
        paired_end=True
      else:
        paired_end=False

      rsem_obj = \
        RSEM_utils(
          rsem_exe_dir=rsem_exe_dir,
          reference_rsem=rsem_ref,
          input_bam=input_bams[0],
          threads=threads,
          use_ephemeral_space=use_ephemeral_space,
          memory_limit=memory_limit)                                            # prepare rsem for run
      rsem_cmd,rsem_output_list,rsem_log_file = \
        rsem_obj.\
          run_rsem_calculate_expression(
            output_dir=work_dir,
            output_prefix=output_prefix,
            paired_end=paired_end,
            strandedness=strandedness,
            options=rsem_options,
            force=force_overwrite)
      if not isinstance(rsem_output_list,list) or \
         len(rsem_output_list)==0:
        raise ValueError('No RSEM output files found')                          # check output files

      self.param('dataflow_params',
                 {'rsem_output':rsem_output_list,
                  'rsem_log_file':rsem_log_file,
                  'seed_date_stamp':seed_date_stamp})                           # pass on rsem output list
      message = \
        'Finished RSEM {0} for {1}'.format(
          project_igf_id,
          sample_igf_id)
      self.post_message_to_slack(message,reaction='pass')                       # send log to slack
      self.post_message_to_ms_team(
          message=message,
          reaction='pass')
      message = \
        'RSEM {0} command: {1}'.format(
          experiment_igf_id,
          rsem_cmd)
      #self.comment_asana_task(task_name=project_igf_id, comment=message)        # send commandline to Asana
    except Exception as e:
      message = \
        'project: {2}, sample:{3}, Error in {0}: {1}'.\
          format(
            self.__class__.__name__,
            e,
            project_igf_id,
            sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      self.post_message_to_ms_team(
          message=message,
          reaction='fail')
      raise