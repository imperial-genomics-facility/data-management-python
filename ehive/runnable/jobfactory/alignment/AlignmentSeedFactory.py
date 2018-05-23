from ehive.runnable.IGFBaseJobFactory import IGFBaseJobFactory

class AlignmentSeedFactory(IGFBaseJobFactory):
  '''
  Seed job factory for alignment and analysis pipeline
  '''
  def param_defaults(self):
    params_dict=super(AlignmentSeedFactory,self).param_defaults()
    params_dict.update({ 'seed_id_label':'seed_id',
                         'seeded_label':'SEEDED',
                         'running_label':'RUNNING',
                         'project_igf_id_label':'project_igf_id',
                         'experiment_igf_id_label':'experiment_igf_id',
                         'seed_status_label':'status',
                       })
    return params_dict

  def run(self):
    '''
    Run method for the seed job factory class of the alignment pipeline
    
    :param igf_session_class: A database session class
    :param pipeline_name: Name of the pipeline
    :param seed_id_label: A text label for the seed_id, default seed_id
    :param seeded_label: A text label for the status seeded in pipeline_seed table, default SEEDED
    :param running_label: A text label for the status running in the pipeline_seed table, default RUNNING
    :param seed_status_label: A text label for the pipeline_seed status column name, default status
    :param project_igf_id_label: A text label for the project_igf_id, default project_igf_id
    :param experiment_igf_id_label: A text label for the experiment_igf_id, default experiment_igf_id
    :returns: A list of dictionary containing the project_igf_ids seed for analysis
    '''
    try:
      igf_session_class = self.param_required('igf_session_class')              # set by base class
      pipeline_name = self.param_required('pipeline_name')
      seed_id_label = self.param_required('seed_id_label')
      seeded_label = self.param_required('seeded_label')
      running_label = self.param_required('running_label')
      project_igf_id_label = self.param_required('project_igf_id_label')
      experiment_igf_id_label = self.param_required('experiment_igf_id_label')
      seed_status_label = self.param_required('seed_status_label')
      
    except:
      message='Error in {0},{1}: {2}'.format(self.__class__.__name__,\
                                             pipeline_name, e)                  # format slack msg
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # send msg to slack
      raise