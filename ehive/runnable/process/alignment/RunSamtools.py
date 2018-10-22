import os
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.fileutils import move_file,get_temp_dir,remove_dir
from igf_data.utils.tools.samtools_utils import run_bam_flagstat,run_bam_idxstat,merge_multiple_bam

class RunSamtools(IGFBaseProcess):
  '''
  A ehive process class for running samtools analysis
  '''
  def param_defaults(self):
    params_dict=super(RunSamtools,self).param_defaults()
    params_dict.update({
        'reference_type':'GENOME_FASTA',
        'threads':4,
        'copy_input':0,
        'analysis_files':[],
        'sorted_by_name':False
      })
    return params_dict

  def run(self):
    '''
    A method for running samtools commands
    
    :param project_igf_id: A project igf id
    :param sample_igf_id: A sample igf id
    :param experiment_igf_id: A experiment igf id
    :param igf_session_class: A database session class
    :param reference_type: Reference genome collection type, default GENOME_FASTA
    :param species_name: species_name
    :param threads: Number of threads to use for Bam to Cram conversion, default 4
    :param base_work_dir: Base workd directory
    :param samtools_command: Samtools command
    :param copy_input: A toggle for copying input file to temp, 1 for True default 0 for False
    '''
    try:
      project_igf_id=self.param_required('project_igf_id')
      sample_igf_id=self.param_required('sample_igf_id')
      experiment_igf_id=self.param_required('experiment_igf_id')
      igf_session_class=self.param_required('igf_session_class')
      species_name=self.param_required('species_name')
      input_file=self.param_required('input_file')
      samtools_exe=self.param_required('samtools_exe')
      reference_type=self.param('reference_type')
      threads=self.param('threads')
      base_work_dir=self.param_required('base_work_dir')
      samtools_command=self.param_required('samtools_command')
      copy_input=self.param('copy_input')
      analysis_files=self.param_required('analysis_files')
      if copy_input==1:
        input_file=self.copy_input_file_to_temp(input_file=input_file)            # copy input to temp dir

      temp_output_dir=get_temp_dir()                                            # get temp work dir
      work_dir_prefix=os.path.join(base_work_dir,
                                   project_igf_id,
                                   sample_igf_id,
                                   experiment_igf_id)
      work_dir=self.get_job_work_dir(work_dir=work_dir_prefix)                  # get a run work dir
      if samtools_command == 'idxstats':
        temp_output=run_bam_idxstat(samtools_exe=samtools_exe,
                                    bam_file=input_file,
                                    output_dir=temp_output_dir,
                                    force=True)                                 # run samtools idxstats
      elif samtools_command == 'flagstat':
        temp_output=run_bam_flagstat(samtools_exe=samtools_exe,
                                     bam_file=input_file,
                                     output_dir=temp_output_dir,
                                     threads=threads,
                                     force=True)                                # run samtools flagstat
      elif samtools_command == 'merge':
        output_prefix=self.param_required('output_prefix')
        sorted_by_name=self.param('sorted_by_name')
        temp_output=os.path.join(work_dir,'{0}_merged.bam'.format(output_prefix))
        merge_multiple_bam(samtools_exe=samtools_exe,
                           input_bam_list=input_file,
                           output_bam_path=temp_output,
                           sorted_by_name=sorted_by_name,
                           threads=threads,
                           force=True)
      else:
        raise ValueError('Samtools command {0} not supported'.\
                         format(samtools_command))

      dest_path=os.path.join(work_dir,
                             os.path.basename(temp_output))
      if dest_path !=temp_output:
        move_file(source_path=temp_output,
                  destinationa_path=dest_path,
                  force=True)

      analysis_files.append(dest_path)
      self.param('dataflow_params',{'analysis_files':analysis_files})           # pass on samtools output list
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