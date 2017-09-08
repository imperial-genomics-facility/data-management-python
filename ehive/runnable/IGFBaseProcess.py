from ehive.runnable.IGFBaseRunnable import IGFBaseRunnable

class IGFBaseProcess(IGFBaseRunnable):
  '''
  Base process class for igf pipelines
  '''
  def param_defaults(self):
    return {'sub_tasks':list() }


  def write_output(self):
    if self.param_is_defined('sub_tasks'):
      sub_tasks = self.param('sub_tasks')   
      self.dataflow(sub_tasks, 1)
