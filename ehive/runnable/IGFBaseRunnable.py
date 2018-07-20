import os, eHive, json
from datetime import datetime
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.task_tracking.igf_slack import IGF_slack
from igf_data.task_tracking.igf_asana import IGF_asana
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.utils.fileutils import get_datestamp_label,get_temp_dir,copy_local_file
from numpy import isin


class IGFBaseRunnable(eHive.BaseRunnable):
  '''
  Base runnable class for IGF pipelines
  '''
  def param_defaults(self):
    return { 'log_slack':True,
             'log_asana':True,
             'sub_tasks':list()
           }


  def fetch_input(self):
    '''
    Fetch input method for base runnable
    
    :param dbconfig: A database configuration json file
    :param log_slack: A toggle for writing logs to slack
    :param log_asana: A toggle for writing logs to asana 
    '''
    try:
      dbconfig = self.param_required('dbconfig')
      dbparams = read_dbconf_json(dbconfig)
      base = BaseAdaptor(**dbparams)
      session_class = base.get_session_class()
      self.param('igf_session_class', session_class)                            # set session class for pipeline

      if self.param('log_slack'):
        slack_config = self.param_required('slack_config')
        igf_slack = IGF_slack(slack_config=slack_config)
        self.param('igf_slack', igf_slack)

      if self.param('log_asana'):
        asana_config = self.param_required('asana_config')
        asana_project_id = self.param_required('asana_project_id')
        igf_asana = IGF_asana(asana_config=asana_config, asana_project_id=asana_project_id)
        self.param('igf_asana', igf_asana)
    except:
      raise

  def run(self):
    pass


  def write_output(self):
    pass


  def post_message_to_slack(self,message,reaction=''):
    '''
    A method for posing message to slack channel

    :param message: A text message
    :param reaction: Optional parameter for slack emoji
    '''
    try:
      if self.param('log_slack'):
        igf_slack = self.param_required('igf_slack')
        igf_slack.post_message_to_channel(message,reaction)
    except:
      raise


  def post_file_to_slack(self,filepath,message):
    '''
    A method for posting message to slack channel
    
    :param filepath: A filepath
    :param message: A message text
    '''
    try:
      if self.param('log_slack'):
        igf_slack = self.param_required('igf_slack')
        igf_slack.post_file_to_channel(message=message,filepath=filepath)
    except:
      raise


  def upload_file_to_asana_task(self,task_name,filepath,comment=None):
    '''
    A base method for uploading file to the asana task
    
    :param task_name: A asana task name
    :param filepath: A filepath
    :param comment: An optional text comment
    '''
    try:
      if self.param('log_asana'):
        igf_asana=self.param_required('igf_asana')
        igf_asana.\
        attach_file_to_asana_task(task_name=task_name,
                                  filepath=filepath,
                                  comment=comment)
    except:
      raise


  def comment_asana_task(self,task_name, comment):
    '''
    A base method for commenting asana task
    
    :param task_name: A task name
    :param comment: A text comment
    :returns: response code asana update
    '''
    try:
      res=None
      if self.param('log_asana'):
        igf_asana=self.param_required('igf_asana')
        res=igf_asana.comment_asana_task(task_name=task_name,
                                         comment=comment)

      return res
    except:
      raise


  def add_asana_notes(self,task_name,notes):
    '''
    A base method for adding asana notes
    
    :param task_name: A task name
    :param notes: A set of text notes
    :returns: response code asana update
    '''
    try:
      res=None
      if self.param('log_asana'):
        igf_asana=self.param_required('igf_asana')
        res=igf_asana.add_notes_for_task(task_name, notes)

      return res
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


  def get_datestamp(self):
    '''
    A method for fetching datestamp
    :returns: A padded string of format YYYYMMDD
    '''
    try:
      datestamp=get_datestamp_label()
      return datestamp
    except:
      raise


  def get_job_work_dir(self,work_dir):
    '''
    A method for getting a job specific work directory
    
    :param work_dir: A work directory path
    :returns: A new job specific directory under the work dir
    '''
    try:
      job_name=self.job_name()
      datestamp=self.get_datestamp()
      work_dir=os.path.join(work_dir,job_name,datestamp)                        # get work directory name
      if not os.path.exists(work_dir):
        os.makedirs(work_dir,mode=0o770)                                        # create work directory

      return work_dir
    except:
      raise


  def copy_input_file_to_temp(self,input_file):
    '''
    A method for copying input file to temp diretory
    
    :param input_file: A input file path
    :returns: A temp file path
    '''
    try:
      if not os.path.exists(input_file):
        raise IOError('File {0} not found'.\
                      format(input_file))

      temp_dir=get_temp_dir()                                                   # get temp dir
      destinationa_path=os.path.join(temp_dir,
                                     os.path.basename(input_file))              # get destination file path
      copy_local_file(source_path=input_file,
                      destinationa_path=destinationa_path,
                      force=True)                                               # copy file to temp dir
      return destinationa_path
    except:
      raise


  def format_tool_options(self,option,separator=None):
    '''
    A method for formatting tool options before running commands tools via 
    subprocess module
    
    :param option: A dictionary or json text as string
    :param separator: A character to use as separator, default is None
    :returns: a formatted list
    '''
    try:
      option_list=list()
      if isinstance(option, str):
        option=json.loads(option.replace('\'','"'))                             # replace ' with " and convert to json dict
        
      if not isinstance(option, dict):
        raise ValueError('expecting param options as dictionary, got {0}'.\
                         format(type(option)))
        
      option_list=[[param,value] if value else [param]
                       for param, value in option.items()]                      # remove empty values
      if separator is None:
        option_list=[col for row in option_list for col in row]                 # flatten sub lists
      else:
        if not isinstance(separator, str):
          raise AttributeError('Expectian a string for param separator, got: {0}'.\
                               format(type(separator)))

        option_list=[separator.join(row) for row in option_list]                # use separator string to combine params

      option_list=list(map(lambda x: str(x),option_list))                       # convert lists values to string
      return option_list
    except:
      raise
