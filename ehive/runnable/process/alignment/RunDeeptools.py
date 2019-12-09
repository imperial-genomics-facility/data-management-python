import os
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.utils.tools.reference_genome_utils import Reference_genome_utils
from igf_data.utils.analysis_collection_utils import Analysis_collection_utils
from igf_data.utils.tools.deeptools_utils import run_plotCoverage,run_bamCoverage,run_plotFingerprint
from igf_data.utils.fileutils import get_datestamp_label

class RunDeeptools(IGFBaseProcess):
  '''
  A ehive process class for running Deeptools analysis
  '''
  def param_defaults(self):
    params_dict=super(RunDeeptools,self).param_defaults()
    params_dict.update({
        'analysis_files':[],
        'output_prefix':None,
        'blacklist_reference_type':'BLACKLIST_BED',
        'load_metrics_to_cram':False,
        'load_signal_bigwig':True,
        'cram_collection_type':'ANALYSIS_CRAM',
        'signal_collection_type':'DEEPTOOLS_BIGWIG',
        'deeptools_params':None,
        'deeptools_bamCov_params': ("--outFileFormat","bigwig"),
        'analysis_name':'Deeptools',
        'collection_table':'experiment',
        'remove_existing_file':True,
        'withdraw_exisitng_collection':True,
        'threads':1,
        'use_ephemeral_space':0,
      })
    return params_dict

  def run(self):
    '''
    A runnable method for running PPQT analysis
    '''
    try:
      project_igf_id = self.param_required('project_igf_id')
      sample_igf_id = self.param_required('sample_igf_id')
      experiment_igf_id = self.param_required('experiment_igf_id')
      igf_session_class = self.param_required('igf_session_class')
      input_files = self.param_required('input_files')
      threads = self.param('threads')
      base_work_dir = self.param_required('base_work_dir')
      base_results_dir = self.param_required('base_results_dir')
      deeptools_command = self.param_required('deeptools_command')
      analysis_files = self.param_required('analysis_files')
      output_prefix = self.param_required('output_prefix')
      load_signal_bigwig = self.param('load_signal_bigwig')
      signal_collection_type = self.param('signal_collection_type')
      blacklist_reference_type = self.param('blacklist_reference_type')
      species_name = self.param('species_name')
      deeptools_params = self.param('deeptools_params')
      deeptools_bamCov_params = self.param('deeptools_bamCov_params')
      collection_table = self.param('collection_table')
      remove_existing_file = self.param('remove_existing_file')
      withdraw_exisitng_collection = self.param('withdraw_exisitng_collection')
      analysis_name = self.param('analysis_name')
      use_ephemeral_space = self.param('use_ephemeral_space')
      seed_date_stamp = self.param_required('date_stamp')
      seed_date_stamp = get_datestamp_label(seed_date_stamp)
      if output_prefix is not None:
        output_prefix = \
          '{0}_{1}'.format(
            output_prefix,
            seed_date_stamp)                                                    # adding datestamp to the output file prefix

      if not isinstance(input_files, list) or \
         len(input_files) == 0:
        raise ValueError('No input file found')

      signal_files = list()
      work_dir_prefix = \
        os.path.join(\
          base_work_dir,
          project_igf_id,
          sample_igf_id,
          experiment_igf_id)
      work_dir = self.get_job_work_dir(work_dir=work_dir_prefix)                # get a run work dir
      ref_genome = \
        Reference_genome_utils(\
          genome_tag=species_name,
          dbsession_class=igf_session_class,
          blacklist_interval_type=blacklist_reference_type)                     # setup ref genome utils
      blacklist_bed = ref_genome.get_blacklist_region_bed()                     # get genome fasta
      if deeptools_command == 'plotCoverage':
        output_raw_counts = \
          '{0}_{1}.raw.txt'.format(output_prefix,'plotCoverage')
        output_raw_counts = \
          os.path.join(\
            work_dir,
            output_raw_counts)
        plotcov_stdout = \
          '{0}_{1}.stdout.txt'.format(output_prefix,'plotCoverage')
        plotcov_stdout = \
          os.path.join(\
            work_dir,
            plotcov_stdout)
        output_plot = \
          '{0}_{1}.pdf'.format(output_prefix,'plotCoverage')
        output_plot = \
          os.path.join(\
            work_dir,
            output_plot)
        deeptools_args = \
          run_plotCoverage(\
            bam_files=input_files,
            output_raw_counts=output_raw_counts,
            plotcov_stdout=plotcov_stdout,
            output_plot=output_plot,
            blacklist_file=blacklist_bed,
            thread=threads,
            use_ephemeral_space=use_ephemeral_space,
            params_list=deeptools_params)
        analysis_files.extend(\
          [output_raw_counts,plotcov_stdout,output_plot])
      elif deeptools_command == 'bamCoverage':
        output_file = \
          '{0}_{1}.bw'.format(output_prefix,'bamCoverage')
        output_file = \
          os.path.join(\
            work_dir,
            output_file)
        if deeptools_params is None:
          deeptools_params = deeptools_bamCov_params

        deeptools_args = \
          run_bamCoverage(\
            bam_files=input_files,
            output_file=output_file,
            blacklist_file=blacklist_bed,
            thread=threads,
            use_ephemeral_space=use_ephemeral_space,
            params_list=deeptools_params)
        if load_signal_bigwig:
          au = \
            Analysis_collection_utils(\
              dbsession_class=igf_session_class,
              analysis_name=analysis_name,
              base_path=base_results_dir,
              tag_name=species_name,
              collection_name=experiment_igf_id,
              collection_type=signal_collection_type,
              collection_table=collection_table)                                # initiate analysis file loading
          output_file_list = \
            au.load_file_to_disk_and_db(\
              input_file_list=[output_file],
              remove_file=remove_existing_file,
              file_suffix='bw',
              withdraw_exisitng_collection=withdraw_exisitng_collection)        # load file to db and disk
          analysis_files.extend(output_file_list)
          signal_files.extend(output_file_list)
        else:
          analysis_files.append(output_file)
      elif deeptools_command == 'plotFingerprint':
        output_raw_counts = \
          '{0}_{1}.raw.txt'.format(output_prefix,'plotFingerprint')
        output_raw_counts = \
          os.path.join(\
            work_dir,
            output_raw_counts)
        output_matrics = \
          '{0}_{1}.metrics.txt'.format(output_prefix,'plotFingerprint')
        output_matrics = \
          os.path.join(\
            work_dir,
            output_matrics)
        output_plot = \
          '{0}_{1}.pdf'.format(output_prefix,'plotFingerprint')
        output_plot = \
          os.path.join(\
            work_dir,
            output_plot)
        deeptools_args = \
          run_plotFingerprint(\
            bam_files=input_files,
            output_raw_counts=output_raw_counts,
            output_matrics=output_matrics,
            output_plot=output_plot,
            blacklist_file=blacklist_bed,
            thread=threads,
            use_ephemeral_space=use_ephemeral_space,
            params_list=deeptools_params)
        analysis_files.extend(\
          [output_raw_counts,output_matrics,output_plot])
      else:
        raise ValueError('Deeptool command {0} is not implemented yet'.\
                         format(deeptools_command))

      self.param('dataflow_params',{'analysis_files':analysis_files,
                                    'signal_files':signal_files,
                                    'seed_date_stamp':seed_date_stamp})         # pass on picard output list
      message = \
        'finished deeptools {0} for {1} {2}'.format(
          deeptools_command,
          project_igf_id,
          sample_igf_id)
      self.post_message_to_slack(message,reaction='pass')                       # send log to slack
      message = \
        'Deeptools {0} command: {1}'.format(
          deeptools_command,
          deeptools_args)
      #self.comment_asana_task(task_name=project_igf_id, comment=message)       # send commandline to Asana
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
      raise
