from ehive.runnable.IGFBaseRunnable import IGFBaseRunnable

class IGFBaseJobFactory(IGFBaseRunnable):
  '''
  Base jobfactory class for igf pipelines
  '''
  def param_defaults(self):
    return {'sub_tasks':list() }


  def write_output(self):
    if self.param_is_defined('sub_tasks'):
      sub_tasks = self.param('sub_tasks')   
      self.dataflow(sub_tasks, 2)

  def test(self):
      pass