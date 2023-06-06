import pandas as pd
from ehive.runnable.IGFBaseJobFactory import IGFBaseJobFactory
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor

class AlignmentSeedFactory(IGFBaseJobFactory):
  '''
  Seed job factory for alignment and analysis pipeline
  '''
  def param_defaults(self):
    params_dict=super(AlignmentSeedFactory,self).param_defaults()
    params_dict.update({ 'seed_id_label':'seed_id',
                         'seeded_label':'SEEDED',
                         'running_label':'RUNNING',
                         'experiment_id_label':'experiment_id',
                         'seed_status_label':'status',
                       })
    return params_dict

  def run(self):
    '''
    Run method for the seed job factory class of the alignment pipeline

    :param igf_session_class: A database session class
    :param pipeline_name: Name of the pipeline
    :param seed_id_label: A text label for the seed_id, default seed_id
    :param seeded_label: A text label for the status seeded in pipeline_seed table, default SEEDED
    :param running_label: A text label for the status running in the pipeline_seed table, default RUNNING
    :param seed_status_label: A text label for the pipeline_seed status column name, default status
    :param experiment_id_label: A text label for the experiment_id, default experiment_id
    :returns: A list of dictionary containing the experiment_igf_ids seed for analysis
    '''
    try:
      igf_session_class = self.param_required('igf_session_class')              # set by base class
      pipeline_name = self.param_required('pipeline_name')
      seed_id_label = self.param_required('seed_id_label')
      seeded_label = self.param_required('seeded_label')
      running_label = self.param_required('running_label')
      experiment_id_label = self.param_required('experiment_id_label')
      seed_status_label = self.param_required('seed_status_label')

      pa = PipelineAdaptor(**{'session_class':igf_session_class})               # get db adaptor
      pa.start_session()                                                        # connect to db
      (pipeseeds_data,table_data) = \
          pa.fetch_pipeline_seed_with_table_data(pipeline_name=pipeline_name,
                                                 table_name='experiment')       # fetch requires entries as list of dictionaries from table for the seeded entries
      if not isinstance(pipeseeds_data,pd.DataFrame) or \
         not isinstance(table_data,pd.DataFrame):
        raise AttributeError('Expecting a pandas dataframe of pipeseed data and received {0}, {1}').\
                             format(type(pipeseeds_data),type(table_data))

      if len(pipeseeds_data.index) == 0 and \
         len(table_data.index) == 0:
        pipeseeds_data[seed_id_label]=pipeseeds_data[seed_id_label].\
                                map(lambda x: int(x))                           # convert pipeseed column type
        table_data[experiment_id_label]=table_data[experiment_id_label].\
                                        map(lambda x: int(x))                   # convert experiment data column type
        merged_data=pd.merge(pipeseeds_data,
                             table_data,
                             how='inner',
                             on=None,
                             left_on=[seed_id_label],
                             right_on=[experiment_id_label],
                             left_index=False,
                             right_index=False)                                 # join dataframes
        dataflow_seed_data=merged_data.\
                           applymap(lambda x: str(x)).\
                           to_dict(orient='records')                            # convert dataframe to string and add as list of dictionaries
        self.param('sub_tasks',dataflow_seed_data)                              # set sub_tasks param for the data flow
        pipeseeds_data[seed_status_label]=pipeseeds_data[seed_status_label].\
                                          map({seeded_label:running_label})     # update seed records in pipeseed table, changed status to RUNNING
        pa.update_pipeline_seed(data=pipeseeds_data.to_dict(orient='records'))  # set pipeline seeds as running

        message='Total {0} new job found for {1}, pipeline: {2}'.\
                 format(len(dataflow_seed_data),self.__class__.__name__,pipeline_name)   # format msg for slack
        self.post_message_to_slack(message,reaction='pass')                     # send update to slack
        self.post_message_to_ms_team(
          message=message,
          reaction='pass')
      else:
        message = \
          '{0}, {1}: no new job created'.\
            format(
              self.__class__.__name__,
              pipeline_name)            # format msg for failed jobs
        self.warning(message)
        self.post_message_to_slack(message,reaction='sleep')                    # post about failed job to slack
        self.post_message_to_ms_team(
          message=message,
          reaction='sleep')
    except Exception as e:
      message = \
        'Error in {0},{1}: {2}'.\
          format(
            self.__class__.__name__,
            pipeline_name,
            e)                                                                  # format slack msg
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # send msg to slack
      self.post_message_to_ms_team(
          message=message,
          reaction='fail')
      raise