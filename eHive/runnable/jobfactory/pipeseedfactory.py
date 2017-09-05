from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from eHive.runnable.igfbaserunnable import IGFBaseJobFactory

class PipeseedFactory(IGFBaseJobFactory):
  '''
  Job factory class for pipeline seed
  '''
  def run(self):
    try:
      subtasks=list()
      igf_session_class = self.param_required('igf_session_class') 
      pipeline_name = self.param_required('pipeline_name')
      pa = PipelineAdaptor(**{'session_class':igf_session_class})
      pa.start_session()
      (pipeseeds_data, table_data) = pa.fetch_pipeline_seed_with_table_data()  # fetch requires entries as list of dictionaries from table for the seeded entries

      if isinstance(table_data, list) and len(table_data)>0:
        self.param('subtasks',table_data)
        pipeseeds_data['status'].map({'SEEDED':'RUNNING'})                     # update seed records in pipeseed table, changed status to RUNNING
        pa.update_pipeline_seed(data=pipeseeds_data)
      else:
        message='{0}: no new jobs created'.format(self.__class__.__name__)  
        self.warning(message)
        if self.param('log_slack'):
          igf_slack = self.param_required('igf_slack')
          igf_slack.post_message_to_channel(message,reaction='fail') 
  
      pa.close_session()
    except Exception as e:
      message='Error in {0}: {1}'.format(self.__class__.__name__, e)
      self.warning(message)
      if self.param('log_slack'):
        igf_slack = self.param_required('igf_slack')
        igf_slack.post_message_to_channel(message,reaction='fail') 

        
