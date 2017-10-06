
from ehive.runnable.IGFBaseJobFactory import IGFBaseJobFactory
class ProjectFastqdirFactory(IGFBaseJobFactory):
  '''
  A job factory for all the fastq dir for a project
  '''
  def run(self):
    try:
      project_fastq=self.param_required('project_fastq')
      seed_data=[{'fastq_dir':fastq_dir} for fastq_dir in project_fastq.key()]  # define seed data
      self.param('sub_tasks',seed_data)                                         # add param for dataflow
    except:
      raise