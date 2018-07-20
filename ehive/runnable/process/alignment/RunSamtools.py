import os
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.fileutils import move_file,get_temp_dir,remove_dir
from igf_data.utils.tools.samtools_utils import run_bam_flagstat,run_bam_idxstat

class RunSamtools(IGFBaseProcess):
  '''
  A ehive process class for running samtools analysis
  '''
  def param_defaults(self):
    params_dict=super(RunSamtools,self).param_defaults()
    params_dict.update({
        'reference_type':'GENOME_FASTA',
        'threads':4,
      })
    return params_dict

  def run(self):
    '''
    A method for running bam to cram conversion
    
    :param project_igf_id: A project igf id
    :param sample_igf_id: A sample igf id
    :param experiment_igf_id: A experiment igf id
    :param igf_session_class: A database session class
    :param reference_type: Reference genome collection type, default GENOME_FASTA
    :param threads: Number of threads to use for Bam to Cram conversion, default 4
    :param base_work_dir: Base workd directory
    :param samtools_command: Samtools command
    '''
    try:
      project_igf_id=self.param_required('project_igf_id')
      sample_igf_id=self.param_required('sample_igf_id')
      experiment_igf_id=self.param_required('experiment_igf_id')
      igf_session_class=self.param_required('igf_session_class')
      species_name=self.param_required('species_name')
      bam_file=self.param_required('bam_file')
      reference_type=self.param('reference_type')
      threads=self.param('threads')
      base_work_dir=self.param_required('base_work_dir')
      samtools_command=self.param_required('samtools_command')
      temp_output_dir=get_temp_dir()                                            # get temp work dir
      work_dir_prefix=os.path.join(base_work_dir,
                                   project_igf_id,
                                   sample_igf_id,
                                   experiment_igf_id)
      work_dir=self.get_job_work_dir(work_dir=work_dir_prefix)                  # get a run work dir
      if samtools_command == 'idxstats':
        temp_output=run_bam_idxstat(bam_file=bam_file,
                                    output_dir=temp_output_dir,
                                    force=True)                                 # run samtools idxstats
      elif samtools_command == 'flagstat':
        temp_output=run_bam_flagstat(bam_file=bam_file,
                                     output_dir=temp_output_dir,
                                     threads=threads,
                                     force=True)                                # run samtools flagstat
      else:
        raise ValueError('Samtools command {0} not supported'.\
                         format(samtools_command))

      dest_path=os.path.join(work_dir_prefix,
                             os.path.basename(temp_output))
      move_file(source_path=temp_output,
                destinationa_path=dest_path,
                force=True)
      self.param('dataflow_params',{samtools_command:dest_path})                # pass on output list
      message='finished samtools {0} for {1} {2}: {3}'.\
              format(samtools_command,
                     project_igf_id,
                     sample_igf_id,
                     dest_path)
      self.post_message_to_slack(message,reaction='pass')                       # send log to slack
      self.comment_asana_task(task_name=project_igf_id, comment=message)        # send comment to Asana
    except Exception as e:
      message='project: {2}, sample:{3}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      project_igf_id,
                                                      sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise