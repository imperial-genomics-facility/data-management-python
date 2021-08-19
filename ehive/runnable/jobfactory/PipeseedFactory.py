import datetime
import pandas as pd
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from ehive.runnable.IGFBaseJobFactory import IGFBaseJobFactory
from igf_data.utils.ehive_utils.pipeseedfactory_utils import get_pipeline_seeds

class PipeseedFactory(IGFBaseJobFactory):
  '''
  Job factory class for pipeline seed
  '''
  def param_defaults(self):
    params_dict = \
      super(PipeseedFactory,self).param_defaults()
    params_dict.\
      update({
        'seed_id_label':'seed_id',
        'seqrun_id_label':'seqrun_id',
        'seeded_label':'SEEDED',
        'running_label':'RUNNING',
        'seqrun_date_label':'seqrun_date',
        'seqrun_igf_id_label':'seqrun_igf_id',
        'seed_status_label':'status',
        'experiment_id_label':'experiment_id',
        'pipeseed_mode':'demultiplexing',
      })
    return params_dict


  def run(self):
    '''
    Run method for the seed job factory class of the all pipelines

    :param igf_session_class: A database session class
    :param pipeline_name: Name of the pipeline
    :param seed_id_label: A text label for the seed_id, default seed_id
    :param seqrun_id_label: A text for seqrun_id column name, default seqrun_id
    :param seqrun_date_label: A text label for the seqrun date, default seqrun_date
    :param seqrun_igf_id_label: A text label for sequencing run igf id, default seqrun_igf_id
    :param seeded_label: A text label for the status seeded in pipeline_seed table, default SEEDED
    :param running_label: A text label for the status running in the pipeline_seed table, default RUNNING
    :param seed_status_label: A text label for the pipeline_seed status column name, default status
    :param experiment_id_label: A text label for the experiment_id, default experiment_id
    :param pipeseed_mode: A text label for pipeline mode, default demultiplexing
      Allowed values are

        *   demultiplexing
        *   alignment

    :returns: A list of dictionary containing the seqrun ids or experiment_igf_ids seed for analysis
    '''
    try:
      dbconnected = False
      igf_session_class = self.param_required('igf_session_class')              # set by base class
      pipeline_name = self.param_required('pipeline_name')
      seeded_label = self.param_required('seeded_label')
      running_label = self.param_required('running_label')
      seed_status_label = self.param_required('seed_status_label')
      pipeseed_mode = self.param_required('pipeseed_mode')

      if pipeseed_mode not in ('demultiplexing','alignment'):
        raise ValueError(
          'Pipeseed_mode {0} not supported'.\
            format(pipeseed_mode))

      pipeseeds_data,seed_data = \
        get_pipeline_seeds(
          pipeseed_mode=pipeseed_mode,
          pipeline_name=pipeline_name,
          igf_session_class=igf_session_class)                                  # fetch pipeseed data from db
      if len(seed_data.index)>0:
        seed_data = \
          seed_data.\
            to_dict(orient='records')                                           # convert dataframe to list of dictionaries
        self.param('sub_tasks',seed_data)                                       # set sub_tasks param for the data flow
        pipeseeds_data[seed_status_label] = \
          pipeseeds_data[seed_status_label].\
            map({seeded_label:running_label})                                   # update seed records in pipeseed table, changed status to RUNNING
        pa = PipelineAdaptor(**{'session_class':igf_session_class})             # get db adaptor
        pa.start_session()                                                      # connect to db
        dbconnected = True
        pa.update_pipeline_seed(
          data=pipeseeds_data.to_dict(orient='records'),
          autosave=False)                                                       # set pipeline seeds as running
        pa.commit_session()                                                     # save changes to db
        pa.close_session()                                                      # close db connection
        dbconnected=False
        message = \
          'Total {0} new job found for {1}, pipeline: {2}'.\
            format(
              len(seed_data),
              self.__class__.__name__,
              pipeline_name)                                                    # format msg
        self.post_message_to_slack(
          message,reaction='pass')                                              # send update to slack
        self.post_message_to_ms_team(
          message=message,
          reaction='pass')                                                      # send update to ms team
      else:
        message = \
          '{0}, {1}: no new job created'.\
            format(
              self.__class__.__name__,
              pipeline_name)                                                    # format msg for failed jobs
        self.warning(message)
        self.post_message_to_slack(
          message,reaction='sleep')                                             # post about failed job to slack
        self.post_message_to_ms_team(
          message=message,
          reaction='sleep')                                                     # send update to ms team
    except Exception as e:
      message = \
        'Error in {0},{1}: {2}'.\
          format(
            self.__class__.__name__,
            pipeline_name, e)                                                   # format slack msg
      self.warning(message)
      self.post_message_to_slack(
        message,reaction='fail')                                                # send msg to slack
      self.post_message_to_ms_team(
        message=message,
        reaction='fail')                                                        # send update to ms team
      if dbconnected:
        pa.rollback_session()                                                   # remove changes from db
        pa.close_session()
      raise                                                                     # mark worker as failed

