from ehive.runnable.IGFBaseJobFactory import IGFBaseJobFactory

class AnalysisFactory(IGFBaseJobFactory):
  '''
  A job factory for running analysis for a list of files
  '''
  def param_defaults(self):
    params_dict=super(AnalysisFactory,self).param_defaults()
    return params_dict

  def run(self):
    try:
      project_igf_id=self.param_required('project_igf_id')
      sample_igf_id=self.param_required('sample_igf_id')
      file_list=self.param_required('file_list')
      seed_data=[{'input_file':input_file}
                    for input_file in file_list]                                # define seed data
      self.param('sub_tasks',seed_data)                                         # add param for dataflow
    except Exception as e:
      message = \
        'project: {2}, sample:{3}, Error in {0}: {1}'.\
          format(
            self.__class__.__name__,
            e,
            project_igf_id,
            sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      self.post_message_to_ms_team(
          message=message,
          reaction='fail')
      raise