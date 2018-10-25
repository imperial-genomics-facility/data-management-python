import os
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.analysis_collection_utils import Analysis_collection_utils


class CollectAnalysisFiles(IGFBaseProcess):
  '''
  '''
  def param_defaults(self):
    params_dict=super(CollectAnalysisFiles,self).param_defaults()
    params_dict.update({
        'force_overwrite':True,
        'analysis_name':'analysis',
        'collection_type':None,
        'collection_table':None,
        'withdraw_exisitng_collection':False,
        'remove_existing_file':False,
        'file_suffix':None
      })
    return params_dict

  def run(self):
    '''
    '''
    try:
      project_igf_id=self.param_required('project_igf_id')
      experiment_igf_id=self.param_required('experiment_igf_id')
      sample_igf_id=self.param_required('sample_igf_id')
      igf_session_class=self.param_required('igf_session_class')
      tag_name=self.param('tag_name')
      input_files=self.param_required('input_files')
      base_result_dir=self.param_required('base_results_dir')
      analysis_name=self.param('analysis_name')
      collection_name=self.param_required('collection_name')
      collection_type=self.param('collection_type')
      collection_table=self.param('collection_table')
      withdraw_exisitng_collection=self.param('withdraw_exisitng_collection')
      remove_existing_file=self.param('remove_existing_file')
      file_suffix=self.param('file_suffix')
      for file in input_files:
        if not os.path.exists(file):
          raise IOError('File {0} not found'.format(file))                      # check analysis files before loading

      au=Analysis_collection_utils(\
           dbsession_class=igf_session_class,
           analysis_name=analysis_name,
           base_path=base_result_dir,
           tag_name=tag_name,
           collection_name=collection_name,
           collection_type=collection_type,
           collection_table=collection_table)                                   # initiate analysis file loading
      output_file_list=au.load_file_to_disk_and_db(\
                            input_file_list=input_files,
                            remove_file=remove_existing_file,
                            file_suffix=file_suffix,
                            withdraw_exisitng_collection=withdraw_exisitng_collection) # load file to db and disk
      self.param('dataflow_params',{'analysis_output_list':output_file_list})   # pass on analysis files to data flow
    except Exception as e:
      message='project: {2}, sample:{3}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      project_igf_id,
                                                      sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise