import os
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.utils.fileutils import move_file,get_temp_dir,remove_dir,get_datestamp_label
from igf_data.utils.tools.samtools_utils import run_bam_flagstat,run_bam_idxstat,merge_multiple_bam,run_bam_stats

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
        'sorted_by_name':False,
        'output_prefix':None,
        'load_metrics_to_cram':False,
        'cram_collection_type':'ANALYSIS_CRAM'
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
    :param threads: Number of threads to use for Bam to Cram conversion, default 4
    :param base_work_dir: Base workd directory
    :param samtools_command: Samtools command
    :param copy_input: A toggle for copying input file to temp, 1 for True default 0 for False
    '''
    try:
      temp_output_dir = False
      project_igf_id = self.param_required('project_igf_id')
      sample_igf_id = self.param_required('sample_igf_id')
      experiment_igf_id = self.param_required('experiment_igf_id')
      igf_session_class = self.param_required('igf_session_class')
      input_files = self.param_required('input_files')
      samtools_exe = self.param_required('samtools_exe')
      reference_type = self.param('reference_type')
      threads = self.param('threads')
      base_work_dir = self.param_required('base_work_dir')
      samtools_command = self.param_required('samtools_command')
      analysis_files = self.param_required('analysis_files')
      output_prefix = self.param_required('output_prefix')
      load_metrics_to_cram = self.param('load_metrics_to_cram')
      cram_collection_type = self.param('cram_collection_type')
      seed_date_stamp = self.param_required('date_stamp')
      seed_date_stamp = get_datestamp_label(seed_date_stamp)
      if output_prefix is not None:
        output_prefix='{0}_{1}'.format(output_prefix,
                                       seed_date_stamp)                         # adding datestamp to the output file prefix

      if not isinstance(input_files, list) or \
         len(input_files) == 0:
        raise ValueError('No input file found')

      if len(input_files)>1:
        raise ValueError('More than one input file found: {0}'.\
                         format(input_files))

      input_file = input_files[0]
      temp_output_dir = get_temp_dir()                                          # get temp work dir
      work_dir_prefix = \
        os.path.join(\
          base_work_dir,
          project_igf_id,
          sample_igf_id,
          experiment_igf_id)
      work_dir = self.get_job_work_dir(work_dir=work_dir_prefix)                # get a run work dir
      samtools_cmdline = ''
      if samtools_command == 'idxstats':
        temp_output,samtools_cmdline = \
          run_bam_idxstat(\
            samtools_exe=samtools_exe,
            bam_file=input_file,
            output_dir=temp_output_dir,
            output_prefix=output_prefix,
            force=True
          )                                                                     # run samtools idxstats
      elif samtools_command == 'flagstat':
        temp_output,samtools_cmdline = \
          run_bam_flagstat(\
            samtools_exe=samtools_exe,
            bam_file=input_file,
            output_dir=temp_output_dir,
            output_prefix=output_prefix,
            threads=threads,
            force=True
          )                                                                     # run samtools flagstat
      elif samtools_command == 'stats':
        temp_output,samtools_cmdline,stats_metrics = \
          run_bam_stats(\
            samtools_exe=samtools_exe,
            bam_file=input_file,
            output_dir=temp_output_dir,
            output_prefix=output_prefix,
            threads=threads,
            force=True
          )                                                                     # run samtools stats
        if load_metrics_to_cram and \
           len(stats_metrics) > 0:
          ca = CollectionAdaptor(**{'session_class':igf_session_class})
          attribute_data = \
          ca.prepare_data_for_collection_attribute(\
            collection_name=experiment_igf_id,
            collection_type=cram_collection_type,
            data_list=stats_metrics
          )
          ca.start_session()
          try:
            ca.create_or_update_collection_attributes(\
              data=attribute_data,
              autosave=False
            )
            ca.commit_session()
            ca.close_session()
          except Exception as e:
            ca.rollback_session()
            ca.close_session()
            raise ValueError('Failed to load data to db: {0}'.\
                           format(e))

      elif samtools_command == 'merge':
        if output_prefix is None:
          raise ValueError('Missing output filename prefix for merged bam')

        sorted_by_name=self.param('sorted_by_name')
        temp_output=\
          os.path.join(\
            work_dir,
            '{0}_merged.bam'.format(output_prefix))
        samtools_cmdline = \
          merge_multiple_bam(\
            samtools_exe=samtools_exe,
            input_bam_list=input_file,
            output_bam_path=temp_output,
            sorted_by_name=sorted_by_name,
            threads=threads,
            force=True
          )
      else:
        raise ValueError('Samtools command {0} not supported'.\
                         format(samtools_command))

      dest_path = \
        os.path.join(\
          work_dir,
          os.path.basename(temp_output))
      if dest_path != temp_output:
        move_file(\
          source_path=temp_output,
          destinationa_path=dest_path,
          force=True)

      analysis_files.append(dest_path)
      self.param('dataflow_params',{'analysis_files':analysis_files})           # pass on samtools output list
      message='finished samtools {0} for {1} {2}'.\
              format(samtools_command,
                     project_igf_id,
                     sample_igf_id)
      self.post_message_to_slack(message,reaction='pass')                       # send log to slack
      message='finished samtools {0} for {1} {2}: {3}'.\
              format(samtools_command,
                     project_igf_id,
                     sample_igf_id,
                     samtools_cmdline)
      #self.comment_asana_task(task_name=project_igf_id, comment=message)        # send comment to Asana
    except Exception as e:
      message='project: {2}, sample:{3}, Error in {0}: {1}'.\
              format(self.__class__.__name__,
                     e,
                     project_igf_id,
                     sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise