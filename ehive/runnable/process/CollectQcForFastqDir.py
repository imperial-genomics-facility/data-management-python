from ehive.runnable.IGFBaseProcess import IGFBaseProcess

class CollectQcForFastqDir(IGFBaseProcess):
  def param_defaults(self):
    params_dict=super(IGFBaseProcess,self).param_defaults()
    return params_dict
  
  def run(self):
    try:
      fastqc_info=self.param_required('fastqc_info')
      fastqscreen_info=self.param_required('fastqscreen_info')
      
      fastqc_files=[fqc_file for fqc_file in fastqc_info.values()]
      fastqscreen_files=[fsr_file for fsr_file in fastqscreen_info.values()]
      
      self.param('dataflow_params',{'qc_outputs': \
                                    {'fastqc':fastqc_files,\
                                     'fastqscreen':fastqscreen_files}})         # repackage qc output for multiqc run
    except Exception as e:
      message='Error in {0}: {1}'.format(self.__class__.__name__, e)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')
      raise