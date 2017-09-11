#!/usr/bin/env python
import pandas as pd
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from ehive.runnable.IGFBaseJobFactory import IGFBaseJobFactory

class PipeseedFactory(IGFBaseJobFactory):
  '''
  Job factory class for pipeline seed
  '''
  def param_defaults(self):
    return {  'seed_id_label':'seed_id',
              'seqrun_id_label':'seqrun_id',
              'log_slack':True
           }


  def run(self):
    try:
      igf_session_class = self.param_required('igf_session_class')
      pipeline_name = self.param_required('pipeline_name')
      seed_id = self.param_required('seed_id_label')
      seqrun_id = self.param_required('seqrun_id_label')
      pa = PipelineAdaptor(**{'session_class':igf_session_class})
      pa.start_session()
      (pipeseeds_data, table_data) = pa.fetch_pipeline_seed_with_table_data(pipeline_name)       # fetch requires entries as list of dictionaries from table for the seeded entries
      if isinstance(pipeseeds_data.to_dict(orient='records'), list)  \
                    and len(pipeseeds_data.to_dict(orient='records'))>0:
        pipeseeds_data[seed_id]=pipeseeds_data[seed_id].astype(int)                              # convert pipeseed column type
        table_data[seqrun_id]=table_data[seqrun_id].astype(int)                                  # convert seqrun data column type
        merged_data=pd.merge(pipeseeds_data,table_data,how='inner',\
                             on=None,left_on=[seed_id],right_on=[seqrun_id],\
                             left_index=False,right_index=False)                                 # join dataframes
        seed_data=merged_data.to_dict(orient='records')                                          # convert dataframe to list of dictionaries
        self.param('sub_tasks',seed_data)                                                        # set sub_tasks param for the data flow
        message='Total {0} new job found for {1}'.format(len(seed_data),self.__class__.__name__)
        self.post_message_to_slack(message,reaction='pass')
        pipeseeds_data['status']=pipeseeds_data['status'].map({'SEEDED':'RUNNING'})              # update seed records in pipeseed table, changed status to RUNNING
        pa.update_pipeline_seed(data=pipeseeds_data.to_dict(orient='records'))                   # set pipeline seeds as running
      else:
        message='{0}: no new job created'.format(self.__class__.__name__)
        self.warning(message)
        self.post_message_to_slack(message,reaction='pass')
 
      pa.close_session()
    except Exception as e:
      message='Error in {0}: {1}'.format(self.__class__.__name__, e)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')
      raise