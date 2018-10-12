import os
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from ehive.runnable.IGFBaseJobFactory import IGFBaseJobFactory

class FetchFastqForRun(IGFBaseJobFactory):
  '''
  A IGF jobfactory runnable for fetching all fastq files for an experiment
  '''
  def param_defaults(self):
    params_dict=super(FetchFastqForRun,self).param_defaults()
    params_dict.update({
      'fastq_collection_type':'demultiplexed_fastq',
      'fastq_collection_table':'run'
      })
    return params_dict

  def run(self):
    try:
      project_igf_id=self.param_required('project_igf_id')
      experiment_igf_id=self.param_required('experiment_igf_id')
      sample_igf_id=self.param_required('sample_igf_id')
      run_igf_id=self.param_required('run_igf_id')
      igf_session_class=self.param_required('igf_session_class')
      fastq_collection_type=self.param('fastq_collection_type')
      fastq_collection_table=self.param('fastq_collection_table')
      ca=CollectionAdaptor(**{'session_class':igf_session_class})
      ca.start_session()
      fastq_files=ca.get_collection_files(collection_name=run_igf_id,
                                          collection_type=fastq_collection_type,
                                          collection_table=fastq_collection_table,
                                          output_mode='dataframe')
      ca.close_session()
      fastq_counts=len(fastq_files.index)
      fastq_list=list(fastq_files['file_path'].values)                          # converting fastq filepaths to a list
      if not isinstance(fastq_list, list) or \
         len(fastq_list)==0:
        raise ValueError('No fastq file found for run {0}'.format(run_igf_id))

      for file in fastq_list:
        if not os.path.exists(file):
          raise IOError('Fastq file path {0} not found for run {1}'.\
                        format(file,run_igf_id))

      self.param('dataflow_params',{'fastq_files_list':fastq_list})             # add fastq filepaths to dataflow
    except Exception as e:
      message='project: {2}, sample:{3}, Error in {0}: {1}'.\
              format(self.__class__.__name__,
                     e,
                     project_igf_id,
                     sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise