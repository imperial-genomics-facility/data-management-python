from ehive.runnable.IGFBaseRunnable import IGFBaseRunnable

class IGFBaseProcess(IGFBaseRunnable):
  '''
  Base process class for igf pipelines
  '''
  def param_defaults(self):
    params_dict=super(IGFBaseProcess,self).param_defaults()
    return params_dict

  def write_output(self):
    if self.param_is_defined('dataflow_params'):
      dataflow_params = self.param('dataflow_params')
      if not isinstance(dataflow_params, dict):
        raise ValueError('expected a dictionary as the dataflow_param and got {0}'.format(type(dataflow_params)))
      self.dataflow(dataflow_params, 1)
