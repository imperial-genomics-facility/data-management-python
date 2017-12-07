from ehive.runnable.IGFBaseProcess import IGFBaseProcess

class CollectQcForFastqDir(IGFBaseProcess):
  def param_defaults(self):
    params_dict=super(IGFBaseProcess,self).param_defaults()
    params_dict.update({
      'remote_fastqc_info':None,
      'remote_fastqs_info':None,
      })
    return params_dict
  
  def run(self):
    try:
      fastq_dir=self.param_required('fastq_dir')
      fastqc_info=self.param_required('fastqc_info')
      fastqscreen_info=self.param_required('fastqscreen_info')
      remote_fastqc_info=self.param('remote_fastqc_info')
      remote_fastqs_info=self.param('remote_fastqs_info')                       # remote file info available only for the known fq
      
      if remote_fastqc_info is not None:
        for fq_file, fqc_file in fastqc_info.items():
          if fq_file in remote_fastqc_info.keys():
            remote_fqc_file=remote_fastqc_info[fq_file]                         # only one remote fqc filepath per fastq file
            fqc_file.update({'remote_fastqc_path':remote_fqc_file, \
                             'fastq_file':fq_file})                             # add remote fastqc path
          
      fastqc_files=[fqc_file for fqc_file in fastqc_info.values()]
      
      if remote_fastqs_info is not None:
        for fq_file, fqs_file in fastqscreen_info.items():
          if fq_file in remote_fastqs_info.keys():
            remote_fqs_file=remote_fastqs_info[fq_file]                         # only one remote fqs filepath per fastq file
            fqs_file.update({'remote_fastqscreen_path':remote_fqs_file, \
                             'fastq_file':fq_file})                             # add remote fastqscreen path
            
      fastqscreen_files=[fsr_file for fsr_file in fastqscreen_info.values()]
      
      self.param('dataflow_params',{'qc_outputs': \
                                    {'fastqc':fastqc_files,\
                                     'fastqscreen':fastqscreen_files}})         # repackage qc output for multiqc run
    except Exception as e:
      message='Error in {0}: {1}'.format(self.__class__.__name__, e)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')
      raise