from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from ehive.runnable.IGFBaseProcess import IGFBaseProcess

class ChangePipelineSeedStatus(IGFBaseProcess):
  def param_defaults(self):
    params_dict=super(IGFBaseProcess,self).param_defaults()
    return params_dict
  
  def run(self):
    try:
      igf_session_class = self.param_required('igf_session_class')              # set by base class
      pipeline_name = self.param_required('pipeline_name')
      seqrun_igf_id=self.param_required('seqrun_igf_id')
      seed_id=self.param_required('seed_id')
      seed_table=self.param_required('seed_table')
      status=self.param_required('status')
      
      pa=PipelineAdaptor(**{'session_class':igf_session_class})
      pa.update_pipeline_seed(data={'pipeline_name':pipeline_name,\
                                    'seed_id':seed_id,\
                                    'seed_table':seed_table,
                                    'status':status.upper()})                   # update seed record in db
      pa.close_session()
      self.post_message_to_slack(message='changing status in {0} for seed {1} as {2}'.\
                                 format(pipeline_name,\
                                        seed_id,\
                                        status.upper()),\
                                 reaction='pass')
    except Exception as e:
      message='seqrun: {2}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise