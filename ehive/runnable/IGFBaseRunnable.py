import os, eHive
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
    session_class = base.get_session_class()
    self.param('igf_session_class', session_class)      # set session class for pipeline

    if self.param('log_slack'):
      slack_config = self.param_required('slack_config')
      igf_slack = IGF_slack(slack_config=slack_config)
      self.param('igf_slack', igf_slack)

    if self.param('log_asana'):
      asana_config = self.param_required('asana_config')
      asana_project_id = self.param_required('asana_project_id')
      igf_asana = IGF_asana(asana_config=asana_config, asana_project_id=asana_project_id)
      self.param('igf_asana', igf_asana)


  def run(self):
    pass
  
  
  def write_output(self):
    pass
  
  
  def post_message_to_slack(self,message,reaction=''):
    '''
    A method for posing message to slack channel
    required params:
    message: A text message
    reaction: Optional parameter for slack emoji
    '''
    if self.param('log_slack'):
      igf_slack = self.param_required('igf_slack')
      igf_slack.post_message_to_channel(message,reaction)
      
      
  def post_file_to_slack(self,filepath,message):
    '''
    A method for posting message to slack channel
    required params:
    filepath: A filepath
    message: A message text
    '''
    try:
      if self.param('log_slack'):
        igf_slack = self.param_required('igf_slack')
        igf_slack.post_file_to_channel(message=message,file=filepath)
    except:
      raise
  
  
  def upload_file_to_asana_task(self,task_name,filepath,comment=None):
    '''
    A base method for uploading file to the asana task
    required params:
    task_name: A asana task name
    filepath: A filepath
    comment: An optional text comment
    '''
    try:
      if self.param('log_asana'):
        igf_asana=self.param_required('igf_asana')
        igf_asana.attach_file_to_asana_task(self,task_name, filepath, comment)
    except:
      raise
  
  
  def comment_asana_task(self,task_name, comment):
    '''
    A base method for commenting asana task
    required params:
    task_name: A task name
    comment: A text comment
    '''
    try:
      if self.param('log_asana'):
        igf_asana=self.param_required('igf_asana')
        igf_asana.comment_asana_task(task_name, comment)
    except:
      raise
    
    
  def get_job_id(self):
    '''
    A method for fetching job process id
    ''' 
    job_pid=os.getpid()
    return job_pid
  
  
  def job_name(self):
    '''
    A method for getting a job name
    '''
    class_name=self.__class__.__name__
    job_id=self.get_job_id()
    job_name='{0}_{1}'.format(class_name,job_id)
    return job_name


class IGFBaseJobFactory(IGFBaseRunnable):
  '''
  Base jobfactory class for igf pipelines
  '''  
  def write_output(self):
    sub_tasks = self.param_required('sub_tasks')   
    self.dataflow(sub_tasks, 2)


class IGFBaseProcess(IGFBaseRunnable):
  '''
  Base process class for igf pipelines
  '''
  def write_output(self):
    sub_tasks = self.param_required('sub_tasks')   
    self.dataflow(sub_tasks, 1)


