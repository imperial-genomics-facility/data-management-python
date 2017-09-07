from ehive.runnable.IGFBaseRunnable import IGFBaseRunnable

class IGFBaseJobFactory(IGFBaseRunnable):
  '''
  Base jobfactory class for igf pipelines
  '''

  def write_output(self):
    sub_tasks = self.param_required('sub_tasks')   
    self.dataflow(sub_tasks, 2)

