from ehive.runnable.IGFBaseJobFactory import IGFBaseJobFactory

class FastqAlignmentFactory(IGFBaseJobFactory):
  '''
  A IGF jobfactory runnable for fetching all active runs for an experiment
  '''
  def param_defaults(self):
    params_dict=super(FastqAlignmentFactory,self).param_defaults()
    return params_dict

  def run(self):
    try:
      project_igf_id=self.param_required('project_igf_id')
      experiment_igf_id=self.param_required('experiment_igf_id')
      sample_igf_id=self.param_required('sample_igf_id')
    except Exception as e:
      message='project: {2}, sample:{3}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      project_igf_id,
                                                      sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise