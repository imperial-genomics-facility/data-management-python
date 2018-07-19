
from ehive.runnable.IGFBaseProcess import IGFBaseProcess

class RunSamtools(IGFBaseProcess):
  '''
  A ehive process class for running samtools analysis
  '''
  def param_defaults(self):
    params_dict=super(ConvertBamToCram,self).param_defaults()
    params_dict.update({
        'reference_type':'GENOME_FASTA',
        'threads':4,
      })
    return params_dict

  def run(self):
    '''
    A method for running bam to cram conversion
    
    :param project_igf_id: A project igf id
    :param sample_igf_id: A sample igf id
    :param igf_session_class: A database session class
    :param reference_type: Reference genome collection type, default GENOME_FASTA
    :param threads: Number of threads to use for Bam to Cram conversion, default 4
    '''
    try:
      project_igf_id=self.param_required('project_igf_id')
      sample_igf_id=self.param_required('sample_igf_id')
      experiment_igf_id=self.param_required('experiment_igf_id')
      igf_session_class=self.param_required('igf_session_class')
      species_name=self.param_required('species_name')
      bam_file=self.param_required('bam_file')
      reference_type=self.param('reference_type')
      threads=self.param('threads')
      
    except Exception as e:
      message='project: {2}, sample:{3}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      project_igf_id,
                                                      sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise