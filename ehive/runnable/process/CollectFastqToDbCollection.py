from ehive.runnable.IGFBaseProcess import IGFBaseProcess

class CollectFastqToDbCollection(IGFBaseProcess):
  '''
  A ehive runnable class for adding fastq files to database
  as the experiment and runs
  '''
  def run(self):
    try:
      fastq_dir=self.param_required('fastq_dir')
      igf_session_class = self.param_required('igf_session_class')
      
    except:
      raise