import eHive
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.task_tracking.igf_slack import IGF_slack
from igf_data.task_tracking.igf_asana import IGF_asana
from igf_data.igfdb.baseadaptor import BaseAdaptor



class IGFBaseRunnable(eHive.BaseRunnable):
  '''
  Base runnable class for IGF pipelines
  '''
  def param_defaults(self):
    return { 'log_slack':True,
             'log_asana':True
           }


  def fetch_input(self):
    dbconfig = self.param_required('dbconfig')
    dbparams = read_dbconf_json(dbconfig)
    base = BaseAdaptor(**dbparams)
    self.param('igf_session_class':base.get_session_class())  # set session class for pipeline

    if self.param('log_slack'):
      slack_config = self.param_required('slack_config')
      igf_slack = IGF_slack(slack_config=slack_config)
      self.param('igf_slack', igf_slack)

    if self.param('log_asana'):
      asana_config = self.param_required('asana_config')
      asana_project_id = self.param_required('asana_project_id')
      igf_asana = IGF_asana(asana_config=asana_config, asana_project_id=asana_project_id)
      self.param('igf_asana', igf_asana)

