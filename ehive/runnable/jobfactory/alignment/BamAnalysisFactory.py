from ehive.runnable.IGFBaseJobFactory import IGFBaseJobFactory

class BamAnalysisFactory(IGFBaseJobFactory):
  '''
  A job factory for running analysis for a lit of bam files
  '''
  def param_defaults(self):
    params_dict=super(BamAnalysisFactory,self).param_defaults()
    return params_dict

  def run(self):
    try:
      project_igf_id=self.param_required('project_igf_id')
      sample_igf_id=self.param_required('sample_igf_id')
      bam_file_list=self.param_required('bam_file_list')
      seed_data=[{'bam_file':bam_file}
                 for bam_file in bam_file_list]                                 # define seed data
      self.param('sub_tasks',seed_data)                                         # add param for dataflow
    except Exception as e:
      message='project: {2}, sample:{3}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      project_igf_id,
                                                      sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise