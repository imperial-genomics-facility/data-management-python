import pandas as pd
from sqlalchemy.sql import column
import os,subprocess,fnmatch, warnings
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_data.task_tracking.igf_slack import IGF_slack
from igf_data.igfdb.igfTables import Project, User, Sample

class Find_and_register_new_project_data:
  '''
  A class for finding new data for project and registering them to the db. 
  Account for new users will be created in irods server and password will be 
  mailed to them.
  required params:
  projet_info_path: A directory path for project info files
  dbconfig: A json dbconfig file
  user_account_template: A template file for user account activation email
  log_slack: Enable or disable sending message to slack, default: True
  slack_config: A slack config json file, required if log_slack is True
  '''
  def __init__(self,projet_info_path,dbconfig,user_account_template, \
               log_slack=True, slack_config=None):
    self.projet_info_path=projet_info_path
    self.user_account_template=user_account_template
    dbparams = read_dbconf_json(dbconfig)
    base=BaseAdaptor(**dbparam)
    self.session_class = base.get_session_class()
    self.log_slack=log_slack
    if log_slack:
      if slack_config is None:
        raise ValueError('Missing slack config file')
      self.igf_slack = IGF_slack(slack_config=slack_config)
  
  
  def process_project_data_and_account(self):
    '''
    A method for finding new project info and registering them to database
    and user account creation
    '''
    try:
      new_project_info_list=self._find_new_project_info()
      if len(new_project_info_list) == 0:
        if log_slack:
          self.igf_slack.post_message_to_channel(message='No project info found',\
                                                 reaction='sleep')
          
      for project_info_file in new_project_info_list:
        try:
          (new_project_info,new_user_info,new_sample_info)=\
            self._read_project_info_and_get_new_entries(project_info_file)      # get new project, user and samples information
        except Exception as e:                                                  # if error found in one file, skip the file
          message='skipped project info file {0}, got error {1}'.\
                  format(project_info_file,e)
          warnings.warn(message)
          if log_slack:
            self.igf_slack.post_message_to_channel(message,reaction='fail')     # send message to slack
    except Exception as e:
      if log_slack:
        message='Error in registering project info: {0}'.format(e)
        self.igf_slack.post_message_to_channel(message,reaction='fail')
      raise
    
    
  def _read_project_info_and_get_new_entries(self,project_info_file):
    '''
    An internal method for processing project info data
    required params:
    project_info_file: A filepath for project_info csv files
    It returns following information
    new_project_info
    new_user_info
    new_sample_info
    '''
    try:
      project_info_data=pd.read_csv(project_info_file)                          # read project info data
      base=BaseAdaptor(**{'session_class':self.session_class})
      required_project_columns=base.get_table_columns(table_name=Project, \
                                             excluded_columns=['project_id'])   # get project columns
      required_user_columns=base.get_table_columns(table_name=User, \
                                             excluded_columns=['user_id'])      # get user columns
      required_sample_columns=base.get_table_columns(table_name=Sample, \
                                             excluded_columns=['sample_id',\
                                                               'project_id'])   # get sample columns
      required_project_user_columns=['project_igf_id','email_id']               # get project user columns
      required_sample_columns.append('project_igf_id')
      project_data=project_info_data.loc[:,required_project_columns]
      user_data=project_info_data.loc[:,required_user_columns]
      project_user_data=project_info_data.loc[:,required_project_user_columns]
      sample_data=project_info_data.loc[:,required_sample_columns]
    except:
      raise
    
    
  def _find_new_project_info(self):
    '''
    An internal method for fetching new project info file
    It returns a list one new project info file
    '''
    try:
      new_project_info_list=list()
      fa=FileAdaptor(**{'session_class':self.session_class})
      fa.start_session()                                                        # connect to db
      for root_path,dirs,files in os.walk(self.projet_info_path, topdown=True):
        for file_path in files:
          if fnmatch.fnmatch(file_name, '*.csv'):                               # only consider csv files
            file_check=fa.check_file_records_file_path(file_path=file_path)     # check for file in db
            if not file_check:
              new_project_info_list.append(os.path.join(root,file))             # collect new project info files
      fa.close_session()                                                        # disconnect db
      return new_project_info_list
    except:
      raise
