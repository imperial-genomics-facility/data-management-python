import pandas as pd
from sqlalchemy.sql import column
import os,subprocess,fnmatch, warnings
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.task_tracking.igf_slack import IGF_slack
from igf_data.igfdb.igfTables import Project, User, Sample
from igf_data.igfdb.useradaptor import UserAdaptor

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
  project_lookup_column: project data lookup column, default project_igf_id
  user_lookup_column: user data lookup column, default email_id
  sample_lookup_column: sample data lookup column, default sample_igf_id
  '''
  def __init__(self,projet_info_path,dbconfig,user_account_template, \
               log_slack=True, slack_config=None,\
               project_lookup_column='project_igf_id',\
               user_lookup_column='email_id',\
               sample_lookup_column='sample_igf_id'):
    self.projet_info_path=projet_info_path
    self.user_account_template=user_account_template
    self.project_lookup_column=project_lookup_column
    self.user_lookup_column=user_lookup_column
    self.sample_lookup_column=sample_lookup_column
    self.log_slack=log_slack
    dbparams = read_dbconf_json(dbconfig)
    base=BaseAdaptor(**dbparam)
    self.session_class = base.get_session_class()
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
          new_data=self._read_project_info_and_get_new_entries(project_info_file) # get new project, user and samples information
          self._check_and_register_data(data=new_data)
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
    
    
  def _check_existing_data(self,data,dbsession,table_name,check_column='EXISTS'):
    '''
    An internal function for checking and registering project info
    '''
    try:
      if not isinstance(data, pd.Seriese):
        raise ValueError('Expecting a data series and got {0}'.format(type(data)))
      if table_name=='project':
        if data[self.project_lookup_column] and \
           data[self.project_lookup_column] == '':
          project_igf_id=data[self.project_lookup_column]
          pa=ProjectAdaptor(**{'session':dbsession})                            # connect to project adaptor
          project_exists=pa.check_project_records_igf_id(project_igf_id)
          if not project_exists:                                                # store data only if project is not existing
            data[check_column]=True
          else:
            data[check_column]=False
          return data
        else:
          raise ValueError('Missing or empty required column {0}'.\
                           format(self.project_lookup_column))
      elif table_name=='user':
        if data[self.user_lookup_column] and \
           data[self.user_lookup_column] == '':
          user_email=data[self.user_lookup_column]
          ua=UserAdaptor(**{'session':dbsession})                               # connect to user adaptor
          user_exists=us.check_user_records_email_id(email_id=user_email)
          if not user_exists:                                                   # store data only if user is not existing
            data[check_column]=True
          else:
            data[check_column]=False
          return data
        else:
          raise ValueError('Missing or empty required column {0}'.\
                           format(self.user_lookup_column))
      elif table_name=='sample':
        if data[self.sample_lookup_column] and \
           data[self.sample_lookup_column] == '':
          sample_igf_id=data[self.sample_lookup_column]
          sa=SampleAdaptor(**{'session':dbsession})                             # connect to sample adaptor
          sample_exists=sa.check_sample_records_igf_id(sample_igf_id)
          if not sample_exists:                                                 # store data only if sample is not existing
            data[check_column]=True
          else:
            data[check_column]=False
          return data
        else:
          raise ValueError('Missing or empty required column {0}'.\
                           format(self.sample_lookup_column))
      elif table_name=='project_user':
        if (data[self.user_lookup_column] and \
           data[self.user_lookup_column] == '') or \
           (data[self.project_lookup_column] and \
           data[self.project_lookup_column] == ''):
          project_igf_id=data[self.project_lookup_column]
          user_email=data[self.user_lookup_column]
          pa=ProjectAdaptor(**{'session':dbsession})                            # connect to project adaptor
          project_user_exists=pa.check_existing_project_user(project_igf_id,\
                                                             email_id=user_email)
          if not project_user_exists:                                           # store data only if sample is not existing
            data[check_column]=True
          else:
            data[check_column]=False
          return data
        else:
          raise ValueError('Missing or empty required column {0}, {1}'.\
                           format(self.project_lookup_column,\
                                  self.user_lookup_column))
      else:
        raise ValueError('table {0} not supported'.format(table_name))
    except:
      raise
  
  
  def _assign_username_and_password(self,data):
    '''
    An internal method for assigning new user account and password
    '''
    pass
  
  
  def _notify_about_new_user_account(self,data):
    '''
    An internal method for sending mail to new user with their password
    '''
    pass
  
  
  def _check_and_register_data(self,data):
    '''
    An internal method for checking and registering data
    '''
    try:
      project_data=data['project_data']
      user_data=data['user_data']
      project_user_data=data['project_user_data']
      sample_data=data['sample_data']
      user_data=user_data.apply(lambda x: \
                                self._assign_username_and_password(x), \
                                axis=1)                                         # check for use account and password
      db_connected=False
      base=BaseAdaptor(**{'session_class':self.session_class})
      base.start_session()                                                      # connect_to db
      db_connected=True
      project_data=project_data.\
                   apply(lambda x: \
                         self._check_existing_data(\
                            data=x,\
                            dbsession=base.session, \
                            table_name='project',
                            check_column='EXISTS'),\
                         axis=1)                                                # get project map
      project_data=project_data[project_data['EXISTS']==False]                  # filter existing projects
      del project_data['EXISTS']                                                # remove extra column
      user_data=user_data.\
                apply(lambda x: \
                      self._check_existing_data(\
                            data=x,\
                            dbsession=base.session, \
                            table_name='user',
                            check_column='EXISTS'),\
                      axis=1)                                                   # get user map
      user_data=user_data[user_data['EXISTS']==False]                           # filter existing users
      del user_data['EXISTS']                                                   # remove extra column
      sample_data=sample_data.\
                   apply(lambda x: \
                         self._check_existing_data(\
                            data=x,\
                            dbsession=base.session, \
                            table_name='sample',
                            check_column='EXISTS'),\
                         axis=1)                                                # get sample map
      sample_data=sample_data[sample_data['EXISTS']==False]                     # filter existing samples
      del sample_data['EXISTS']                                                 # remove extra column
      project_user_data=project_user_data.\
                        apply(lambda x: \
                         self._check_existing_data(\
                            data=x,\
                            dbsession=base.session, \
                            table_name='project_user',
                            check_column='EXISTS'),\
                         axis=1)                                                # get project user map
      project_user_data=project_user_data[project_user_data['EXISTS']==False]   # filter existing project user
      del project_user_data['EXISTS']                                           # remove extra column
      
      if len(project_data.index) > 0:                                           # store new projects
        pass
      
      if len(user_data.index) > 0:                                              # store new users
        pass
      
      if len(project_user_data.index) > 0:                                      # store new project users
        pass
      
      if len(sample_data.index) > 0:                                            # store new samples
        pass
      
    except:
      if db_connected:
        base.rollback_session()
      raise
    else:
      if db_connected:
        base.commit_session()
        if len(user_data.index) > 0:
          user_data.apply(lambda x: self._notify_about_new_user_account(x),\
                          axis=1)                                               # send mail to new user with their password and forget it
    finally:
      if db_connected:
        base.close_session()
  

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
      project_data=project_info_data.loc[:,required_project_columns]            # get data from project table
      user_data=project_info_data.loc[:,required_user_columns]                  # get data from user table
      project_user_data=project_info_data.loc[:,required_project_user_columns]  # get data for project user table
      sample_data=project_info_data.loc[:,required_sample_columns]              # get data for sample table
      project_data=project_data.drop_duplicates()
      user_data=user_data.drop_duplicates()
      project_user_data=project_user_data.drop_duplicates()
      sample_data=sample_data.drop_duplicates()                                 # remove duplicate entries
      if self.project_lookup_column not in project_data.column:
        raise ValueError('Missing required column: {0}'.\
                         format(self.project_lookup_column))
      if self.user_lookup_column not in user_data.column:
        raise ValueError('Missing required column: {0}'.\
                         format(self.user_lookup_column))
      if self.sample_lookup_column not in sample_data.column:
        raise ValueError('Missing required column: {0}'.\
                         format(self.sample_lookup_column))                     # check if required columns are present in the dataframe
        
      return {'project_data':project_data,\
              'user_data':user_data,\
              'project_user_data':project_user_data,\
              'sample_data':sample_data}
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
