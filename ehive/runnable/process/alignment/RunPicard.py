
from ehive.runnable.IGFBaseProcess import IGFBaseProcess

class RunPicard(IGFBaseProcess):
  def param_defaults(self):
    params_dict=super(ProcessCellrangerCountOutput,self).param_defaults()
    params_dict.update({
        'reference_type':'GENOME_FASTA',
      })
    return params_dict

  def run(self):
    try:
      project_igf_id=self.param_required('project_igf_id')
      experiment_igf_id=self.param_required('experiment_igf_id')
      sample_igf_id=self.param_required('sample_igf_id')
      java_exe=self.param_required('java_exe')
      picard_jar=self.param_required('picard_jar')
      input_file=self.param_required('input_file')
      picard_command=self.param_required('picard_command')
      igf_session_class=self.param_required('igf_session_class')
      species_name=self.param('species_name')
      reference_type=self.param('reference_type')
    except Exception as e:
      message='project: {2}, sample:{3}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      project_igf_id,
                                                      sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise