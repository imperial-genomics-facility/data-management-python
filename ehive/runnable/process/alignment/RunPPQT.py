import os
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.utils.tools.ppqt_utils import Ppqt_tools
from igf_data.utils.fileutils import get_datestamp_label
from igf_data.utils.analysis_collection_utils import Analysis_collection_utils

class RunPPQT(IGFBaseProcess):
  '''
  A ehive process class for running Phantopeakqualtools (PPQT) analysis
  '''
  def param_defaults(self):
    params_dict=super(RunPPQT,self).param_defaults()
    params_dict.update({
        'analysis_files':[],
        'output_prefix':None,
        'load_metrics_to_cram':False,
        'cram_collection_type':'ANALYSIS_CRAM',
        'ppqt_collection_type':'PPQT_REPORT',
        'collection_table':'experiment',
        'analysis_name':None,
        'force_overwrite':True,
        'threads':0,,
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
      rscript_path = self.param_required('rscript_path')
      ppqt_exe = self.param_required('ppqt_exe')
      base_work_dir = self.param_required('base_work_dir')
      base_result_dir = self.param_required('base_result_dir')
      library_strategy = self.param_required('library_strategy')
      analysis_files = self.param_required('analysis_files')
      output_prefix = self.param_required('output_prefix')
      species_name = self.param_required('species_name')
      analysis_name = self.param('analysis_name')
      seed_date_stamp = self.param_required('date_stamp')
      load_metrics_to_cram = self.param('load_metrics_to_cram')
      ppqt_collection_type = self.param('ppqt_collection_type')
      cram_collection_type = self.param('cram_collection_type')
      collection_table = self.param('collection_table')
      force_overwrite = self.param('force_overwrite')
      use_ephemeral_space = self.param('use_ephemeral_space')
      threads = self.param('threads')
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

      if analysis_name is None:
        analysis_name = library_strategy                                        # use library_strategy as default analysis_name

      input_file = input_files[0]
      work_dir_prefix = \
        os.path.join(\
          base_work_dir,
          project_igf_id,
          sample_igf_id,
          experiment_igf_id)
      work_dir = self.get_job_work_dir(work_dir=work_dir_prefix)                # get a run work dir
      ppqt_obj = \
        Ppqt_tools(\
          rscript_path=rscript_path,
          ppqt_exe=ppqt_exe,
          use_ephemeral_space=use_ephemeral_space,
          threads=threads)
      ppqt_cmd,spp_output, pdf_output,spp_data = \
        ppqt_obj.run_ppqt(\
          input_bam=input_file,
          output_dir=work_dir,
          output_spp_name='{0}_{1}.spp.out'.format(output_prefix,'PPQT'),
          output_pdf_name='{0}_{1}.spp.pdf'.format(output_prefix,'PPQT'))
      analysis_files.append(spp_output)
      au = \
        Analysis_collection_utils(\
          dbsession_class=igf_session_class,
          analysis_name=analysis_name,
          tag_name=species_name,
          collection_name=experiment_igf_id,
          collection_type=ppqt_collection_type,
          collection_table=collection_table,
          base_path=base_result_dir)
      output_ppqt_list = \
        au.load_file_to_disk_and_db(\
          input_file_list=[pdf_output],
          file_suffix='pdf',
          withdraw_exisitng_collection=force_overwrite)                         # load file to db and disk
      if load_metrics_to_cram and \
         len(spp_data) > 0:
        ca = CollectionAdaptor(**{'session_class':igf_session_class})
        attribute_data = \
          ca.prepare_data_for_collection_attribute(\
            collection_name=experiment_igf_id,
            collection_type=cram_collection_type,
            data_list=spp_data)
        ca.start_session()
        try:
          ca.create_or_update_collection_attributes(\
            data=attribute_data,
            autosave=False)
          ca.commit_session()
          ca.close_session()
        except Exception as e:
          ca.rollback_session()
          ca.close_session()
          raise ValueError('Failed to load data to db: {0}'.\
                           format(e))

      self.param('dataflow_params',{'analysis_files':analysis_files,
                                    'output_ppqt_list':output_ppqt_list})       # pass on samtools output list
      message='finished PPQT for {0} {1}'.\
              format(project_igf_id,
                     sample_igf_id)
      self.post_message_to_slack(message,reaction='pass')                       # send log to slack
      message='finished PPQT for {0} {1}: {2}'.\
              format(project_igf_id,
                     sample_igf_id,
                     ppqt_cmd)
      self.comment_asana_task(task_name=project_igf_id, comment=message)        # send comment to Asana
    except Exception as e:
      message='project: {2}, sample:{3}, Error in {0}: {1}'.\
              format(self.__class__.__name__,
                     e,
                     project_igf_id,
                     sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise