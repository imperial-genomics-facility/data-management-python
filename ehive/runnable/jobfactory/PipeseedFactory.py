import pandas as pd
import datetime
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from ehive.runnable.IGFBaseJobFactory import IGFBaseJobFactory

class PipeseedFactory(IGFBaseJobFactory):
  '''
  Job factory class for pipeline seed
  '''
  def param_defaults(self):
    params_dict=super(PipeseedFactory,self).param_defaults()
    params_dict.update({ 'seed_id_label':'seed_id',
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
                          Allowed values are demultiplexing and alignment
    :returns: A list of dictionary containing the seqrun ids or experiment_igf_ids seed for analysis
    '''
    try:
      dbconnected=False
      igf_session_class = self.param_required('igf_session_class')              # set by base class
      pipeline_name = self.param_required('pipeline_name')
      seed_id_label = self.param_required('seed_id_label')
      seqrun_id_label = self.param_required('seqrun_id_label')
      seeded_label=self.param_required('seeded_label')
      running_label=self.param_required('running_label')
      seqrun_date_label=self.param_required('seqrun_date_label')
      seqrun_igf_id_label=self.param_required('seqrun_igf_id_label')
      seed_status_label=self.param_required('seed_status_label')
      experiment_id_label = self.param_required('experiment_id_label')
      pipeseed_mode=self.param_required('pipeseed_mode')

      if pipeseed_mode not in ('demultiplexing','alignment'):
        raise ValueError('Pipeseed_mode {0} not supported'.format(pipeseed_mode))

      pa = PipelineAdaptor(**{'session_class':igf_session_class})               # get db adaptor
      pa.start_session()                                                        # connect to db
      dbconnected=True
      (pipeseeds_data, table_data) = \
              pa.fetch_pipeline_seed_with_table_data(pipeline_name)             # fetch requires entries as list of dictionaries from table for the seeded entries
      
      if not isinstance(pipeseeds_data,pd.DataFrame) or \
         not isinstance(table_data,pd.DataFrame):
        raise AttributeError('Expecting a pandas dataframe of pipeseed data and received {0}, {1}').\
                             format(type(pipeseeds_data),type(table_data))

      if len(pipeseeds_data.index) == 0 and \
         len(table_data.index) == 0:
        pipeseeds_data[seed_id_label]=pipeseeds_data[seed_id_label].\
                                      map(lambda x: int(x))                     # convert pipeseed column type
        if pipeseed_mode=='demultiplexing':
          table_data[seqrun_id_label]=table_data[seqrun_id_label].\
                                      map(lambda x: int(x))                     # convert seqrun data column type
          merged_data=pd.merge(pipeseeds_data,
                               table_data,
                               how='inner',
                               on=None,
                               left_on=[seed_id_label],
                               right_on=[seqrun_id_label],
                               left_index=False,
                               right_index=False)                               # join dataframes
          merged_data[seqrun_date_label]=merged_data[seqrun_igf_id_label].\
                    map(lambda x:  self._get_date_from_seqrun(seqrun_igf_id=x)) # get seqrun date from seqrun id
        elif pipeseed_mode=='alignment':
          table_data[experiment_id_label]=table_data[experiment_id_label].\
                                          map(lambda x: int(x))                 # convert experiment data column type
          merged_data=pd.merge(pipeseeds_data,
                               table_data,
                               how='inner',
                               on=None,
                               left_on=[seed_id_label],
                               right_on=[experiment_id_label],
                               left_index=False,
                               right_index=False)                               # join dataframes

        seed_data=merged_data.applymap(lambda x: str(x)).\
                              to_dict(orient='records')                         # convert dataframe to string and add as list of dictionaries
        self.param('sub_tasks',seed_data)                                       # set sub_tasks param for the data flow
        pipeseeds_data[seed_status_label]=pipeseeds_data[seed_status_label].\
                                          map({seeded_label:running_label})     # update seed records in pipeseed table, changed status to RUNNING
        pa.update_pipeline_seed(data=pipeseeds_data.to_dict(orient='records'),
                                autosave=False)                                 # set pipeline seeds as running
        pa.commit_session()                                                     # save changes to db
        message='Total {0} new job found for {1}, pipeline: {2}'.\
                format(len(seed_data),self.__class__.__name__,pipeline_name)    # format msg for slack
        self.post_message_to_slack(message,reaction='pass')                     # send update to slack
      else:
        message='{0}, {1}: no new job created'.format(self.__class__.__name__,\
                                                      pipeline_name)            # format msg for failed jobs
        self.warning(message)
        self.post_message_to_slack(message,reaction='sleep')                    # post about failed job to slack

    except Exception as e:
      message='Error in {0},{1}: {2}'.format(self.__class__.__name__,\
                                             pipeline_name, e)                  # format slack msg
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # send msg to slack
      if dbconnected:
        pa.rollback_session()                                                   # remove changes from db

      raise                                                                     # mark worker as failed
    finally:
      if pa:
        pa.close_session()                                                      # close db session if its open


  def _get_date_from_seqrun(self,seqrun_igf_id):
    '''
    An internal method for extracting sequencing run date from seqrun_igf_id
    
    :param seqrun_igf_id: A sequencing run id , eg 171001_XXX_XXX-XXX
    :returns: A datetime object with the seqrun date
    '''
    seqrun_date=seqrun_igf_id.split('_')[0]                                     # set first part of the string, e.g., 171001_XXX_XXX-XXX
    seqrun_date=datetime.datetime.strptime(seqrun_date,'%y%m%d').date()         # convert it to date object
    return seqrun_date
