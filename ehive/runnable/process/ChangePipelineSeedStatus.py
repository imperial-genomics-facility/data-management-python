from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from ehive.runnable.IGFBaseProcess import IGFBaseProcess

class ChangePipelineSeedStatus(IGFBaseProcess):
  def param_defaults(self):
    params_dict=super(ChangePipelineSeedStatus,self).param_defaults()
    return params_dict
  
  def run(self):
    try:
      igf_session_class = self.param_required('igf_session_class')              # set by base class
      pipeline_name = self.param_required('pipeline_name')
      igf_id = self.param_required('igf_id')
      task_id = self.param_required('task_id')
      seed_id = self.param_required('seed_id')
      seed_table = self.param_required('seed_table')
      new_status = self.param_required('new_status')

      pa = PipelineAdaptor(**{'session_class':igf_session_class})
      pa.start_session()                                                        # connect to db
      pa.update_pipeline_seed(\
        data=[{'pipeline_name':pipeline_name,
               'seed_id':int(seed_id),
               'seed_table':seed_table,
               'status':new_status.upper()}])                                   # update seed record in db
      pa.close_session()                                                        # close db connection
      message = \
        'changing status in {0} for seed {1} as {2}'.\
        format(\
          pipeline_name,
          seed_id,
          new_status.upper())                                                   # format message
      self.post_message_to_slack(message, reaction='pass')                      # send message to slack
      self.comment_asana_task(task_name=task_id, comment=message)               # send message to asana
      self.post_message_to_ms_team(
          message=message,
          reaction='pass')
    except Exception as e:
      message = \
        'seqrun: {2}, Error in {0}: {1}'.\
          format(\
            self.__class__.__name__,
            e,
            igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      self.post_message_to_ms_team(
          message=message,
          reaction='fail')
      raise