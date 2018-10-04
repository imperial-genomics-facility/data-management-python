from igf_data.igfdb.experimentadaptor import ExperimentAdaptor
from ehive.runnable.IGFBaseJobFactory import IGFBaseJobFactory

class RunFactory(IGFBaseJobFactory):
  '''
  A IGF jobfactory runnable for fetching all active runs for an experiment
  '''
  def param_defaults(self):
    params_dict=super(RunFactory,self).param_defaults()
    return params_dict

  def run(self):
    try:
      project_igf_id=self.param_required('project_igf_id')
      experiment_igf_id=self.param_required('experiment_igf_id')
      sample_igf_id=self.param_required('sample_igf_id')
      igf_session_class=self.param_required('igf_session_class')
      ea=ExperimentAdaptor(**{'session_class':igf_session_class})
      ea.start_session()
      runs=ea.fetch_runs_for_igf_id(experiment_igf_id=experiment_igf_id,
                                    include_active_runs=True,
                                    output_mode='dataframe')                    # fetch active runs for an experiment
      ea.close_session()
      runs=list(runs['run_igf_id'].values)                                      # convert run ids to a list
      self.param('sub_tasks',runs)                                              # pass on run factory output list
    except Exception as e:
      message='project: {2}, sample:{3}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      project_igf_id,
                                                      sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise