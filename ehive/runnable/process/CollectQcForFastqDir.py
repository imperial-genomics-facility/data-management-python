from ehive.runnable.IGFBaseProcess import IGFBaseProcess

class CollectQcForFastqDir(IGFBaseProcess):
  def param_defaults(self):
    params_dict=IGFBaseProcess.param_defaults()
    return params_dict
  
  def run(self):
    try:
      fastqc_info=self.param_required('fastqc_info')
      fastqscreen_info=self.param_required('fastqscreen_info')
      self.param('dataflow_params',{'qc_outputs': \
                                    {'fastqc':fastqc_info,\
                                     'fastqscreen':fastqscreen_info}})          # repackage qc output for multiqc run
    except Exception as e:
      message='Error in {0}: {1}'.format(self.__class__.__name__, e)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')
      raise