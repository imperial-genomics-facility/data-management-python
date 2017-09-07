from ehive.runnable.IGFBaseRunnable import IGFBaseRunnable

class IGFBaseProcess(IGFBaseRunnable):
  '''
  Base process class for igf pipelines
  '''
  def param_defaults(self):
    return {'sub_tasks':list() }


  def write_output(self):
    sub_tasks = self.param_required('sub_tasks')   
    self.dataflow(sub_tasks, 1)
