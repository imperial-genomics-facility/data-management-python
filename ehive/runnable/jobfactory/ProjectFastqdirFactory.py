
from ehive.runnable.IGFBaseJobFactory import IGFBaseJobFactory
class ProjectFastqdirFactory(IGFBaseJobFactory):
  '''
  A job factory for all the fastq dir for a project
  '''
  
  def param_defaults(self):
    params_dict=super(ProjectFastqdirFactory,self).param_defaults()
    return params_dict
  
  def run(self):
    try:
      project_fastq=self.param_required('project_fastq')
      seqrun_igf_id=self.param_required('seqrun_igf_id')
      seed_data=[{'fastq_dir':fastq_dir} for fastq_dir in project_fastq.keys()]  # define seed data
      self.param('sub_tasks',seed_data)                                         # add param for dataflow
    except Exception as e:
      message='seqrun: {2}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      self.post_message_to_ms_team(
          message=message,
          reaction='fail')
      raise