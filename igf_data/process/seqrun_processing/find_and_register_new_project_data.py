import pandas as pd
from shlex import quote
import os,subprocess,fnmatch,warnings,string
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.task_tracking.igf_slack import IGF_slack
from igf_data.igfdb.igfTables import Project, User, Sample
from igf_data.igfdb.useradaptor import UserAdaptor
from igf_data.utils.fileutils import get_temp_dir, remove_dir
from jinja2 import Environment, FileSystemLoader,select_autoescape

class Find_and_register_new_project_data:
  '''
  A class for finding new data for project and registering them to the db. 
  Account for new users will be created in irods server and password will be 
  mailed to them.
  required params:
  projet_info_path: A directory path for project info files
  dbconfig: A json dbconfig file
  check_hpc_user: Guess the hpc user name, True or False, default: False
  hpc_user: A hpc user name, default is None
  hpc_address: A hpc host address, default is None
  ldap_server: A ldap server address for search, default is None
  user_account_template: A template file for user account activation email
  log_slack: Enable or disable sending message to slack, default: True
  slack_config: A slack config json file, required if log_slack is True
  project_lookup_column: project data lookup column, default project_igf_id
  user_lookup_column: user data lookup column, default email_id
  sample_lookup_column: sample data lookup column, default sample_igf_id
  data_authority_column: data authority column name, default data_authority
  setup_irods: Setup irods account for user, default is True
  notify_user: Send email notification to user, default is True
  '''
  def __init__(self,projet_info_path,dbconfig,user_account_template, \
               log_slack=True, slack_config=None,\
               check_hpc_user=False, hpc_user=None,hpc_address=None,\
               ldap_server=None,\
               setup_irods=True,\
               notify_user=True,\
               project_lookup_column='project_igf_id',\
               user_lookup_column='email_id',\
               data_authority_column='data_authority',\
               sample_lookup_column='sample_igf_id'):
    try:
      self.projet_info_path=projet_info_path
      self.user_account_template=user_account_template
      self.project_lookup_column=project_lookup_column
      self.user_lookup_column=user_lookup_column
      self.sample_lookup_column=sample_lookup_column
      self.data_authority_column=data_authority_column
      self.log_slack=log_slack
      dbparams = read_dbconf_json(dbconfig)
      base=BaseAdaptor(**dbparams)
      self.session_class = base.get_session_class()
      self.setup_irods=setup_irods
      self.notify_user=notify_user
      self.check_hpc_user=check_hpc_user
      self.hpc_user=hpc_user
      self.hpc_address=hpc_address
      self.ldap_server=ldap_server
      if log_slack and slack_config is None:
        raise ValueError('Missing slack config file')
      elif log_slack and slack_config:
        self.igf_slack = IGF_slack(slack_config=slack_config)
      
      if check_hpc_user and (hpc_user is None or \
                             hpc_address is None or \
                             ldap_server is None):
        raise ValueError('Hpc user {0} address {1}, and ldap server {2} are required for check_hpc_user'.\
                         format(hpc_user,hpc_address,ldap_server))
    except:
      raise
  
  
  def process_project_data_and_account(self):
    '''
    A method for finding new project info and registering them to database
    and user account creation
    '''
    try:
      new_project_info_list=self._find_new_project_info()
      if len(new_project_info_list) == 0:
        if self.log_slack:
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
          if self.log_slack:
            self.igf_slack.post_message_to_channel(message,reaction='fail')     # send message to slack
    except Exception as e:
      if self.log_slack:
        message='Error in registering project info: {0}'.format(e)
        self.igf_slack.post_message_to_channel(message,reaction='fail')
      raise
    
    
  def _check_existing_data(self,data,dbsession,table_name,check_column='EXISTS'):
    '''
    An internal function for checking and registering project info
    required params:
    data: A pandas data series
    dbsession: A sqlalchemy database session object
    table_name: A database table name
    check_column: Column name for existing data
    '''
    try:
      if not isinstance(data, pd.Series):
        raise ValueError('Expecting a data series and got {0}'.format(type(data)))
      if table_name=='project':
        if self.project_lookup_column in data and \
           not pd.isnull(data[self.project_lookup_column]):
          project_igf_id=data[self.project_lookup_column]
          pa=ProjectAdaptor(**{'session':dbsession})                            # connect to project adaptor
          project_exists=pa.check_project_records_igf_id(project_igf_id)
          if project_exists:                                                # store data only if project is not existing
            data[check_column]=True
          else:
            data[check_column]=False
          return data
        else:
          raise ValueError('Missing or empty required column {0}'.\
                           format(self.project_lookup_column))
      elif table_name=='user':
        if self.user_lookup_column in data and \
           not pd.isnull(data[self.user_lookup_column]):
          user_email=data[self.user_lookup_column]
          ua=UserAdaptor(**{'session':dbsession})                               # connect to user adaptor
          user_exists=ua.check_user_records_email_id(email_id=user_email)
          if user_exists:                                                   # store data only if user is not existing
            data[check_column]=True
          else:
            data[check_column]=False
          return data
        else:
          raise ValueError('Missing or empty required column {0}'.\
                           format(self.user_lookup_column))
      elif table_name=='sample':
        if self.sample_lookup_column in data and \
           not pd.isnull(data[self.sample_lookup_column]):
          sample_igf_id=data[self.sample_lookup_column]
          sa=SampleAdaptor(**{'session':dbsession})                             # connect to sample adaptor
          sample_exists=sa.check_sample_records_igf_id(sample_igf_id)
          if sample_exists:                                                 # store data only if sample is not existing
            data[check_column]=True
          else:
            data[check_column]=False
          return data
        else:
          raise ValueError('Missing or empty required column {0}'.\
                           format(self.sample_lookup_column))
      elif table_name=='project_user':
        if self.user_lookup_column in data and \
            not pd.isnull(data[self.user_lookup_column]) and \
           self.project_lookup_column in data and \
            not pd.isnull(data[self.project_lookup_column]):
          project_igf_id=data[self.project_lookup_column]
          user_email=data[self.user_lookup_column]
          pa=ProjectAdaptor(**{'session':dbsession})                            # connect to project adaptor
          project_user_exists=pa.check_existing_project_user(project_igf_id,\
                                                             email_id=user_email)
          if self.data_authority_column not in data or \
             pd.isnull(data[self.data_authority_column]):
            data[self.data_authority_column]='T'                                # set user as data authority
            
          if project_user_exists:                                           # store data only if sample is not existing
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
  
  
  def _notify_about_new_user_account(self,data,user_col='username',\
                           password_col='password',hpc_user_col='hpc_username',\
                           name_col='name',email_id_col='email_id'):
    '''
    An internal method for sending mail to new user with their password
    required params:
    data: A pandas series containing user data
    user_col: Column name for username, default username
    password_col: Column name for password, default password
    hpc_user_col: Column name for hpc_username, default hpc_username
    name_col: Column name for name, default name
    email_id_col: Column name for email id, default email_id
    '''
    try:
      if not isinstance(data, pd.Series):
        raise ValueError('Expecting a pandas series and got {0}'.\
                         format(type(data)))
      username=data[user_col]
      fullname=data[name_col]
      password=data[password_col]
      email_id=data[email_id_col]
      
      if hpc_user_col not in data or pd.isnull(data[hpc_user_col]):             # send email only to non-hpc users
        template_dir=os.path.dirname(self.user_account_template)
        template_env=Environment(loader=FileSystemLoader(searchpath=template_dir), \
                                 autoescape=select_autoescape(['html','xml']))  # set template env
        template_file=template_env.\
                      get_template(os.path.basename(self.user_account_template))
        temp_work_dir=get_temp_dir()                                            # get a temp dir
        report_output_file=os.path.join(temp_work_dir,'email_template.txt')
        template_file.\
          stream(userEmail=email_id, \
                 fullName=fullname,\
                 userName=username,\
                 userPass=password,\
                ).\
          dump(report_output_file)
        read_cmd=['cat', quote(report_output_file)]
        proc=subprocess.Popen(read_cmd, stdout=subprocess.PIPE)
        sendmail_cmd=['sendmail', '-t']
        subprocess.check_call(sendmail_cmd,stdin=proc.stdout)
        proc.stdout.close()
        remove_dir(temp_work_dir)
    except:
      raise
  
  
  @staticmethod
  def _get_user_password(password_length=12):
    '''
    An internal staticmethod for generating random password
    required params:
    password_length: Required length of password, default 12
    '''
    try:
      new_password=None                                                         # default value of the new password is None
      symbols='^&%$@#!'                                                         # allowed symbols in password
      chars=string.ascii_lowercase+\
            string.ascii_uppercase+\
            string.digits+\
            symbols                                                             # a string of lower case and upper case letters, digits and symbols
      while new_password is None or \
            new_password[0] in string.digits or \
            new_password[0] in symbols:                                         # password can't be None or starts with digit or a symbol
        new_password=''.join([chars[ord(os.urandom(1)) % len(chars)] \
                              for i in range(password_length)])                 # assign a new random password
      return new_password
    except:
      raise
    
    
  @staticmethod
  def _setup_irods_account(data,user_col='username',\
                           password_col='password',\
                           hpc_user_col='hpc_username',\
                          ):
    '''
    An internal staticmethod for creating new user account in irods
    required params:
    data: A pandas series containing user data
    user_col: Column name for username, deffault username
    password_col: Column name for password, default password
    hpc_user_col: Column name for hpc_username, default hpc_username
    '''
    try:
      if not isinstance(data, pd.Series):
        raise ValueError('Expecting a pandas series and got {0}'.\
                         format(type(data)))
      
      if user_col not in data or pd.isnull(data[user_col]):
        raise ValueError('Missing required username')
      
      if (hpc_user_col not in data or pd.isnull(data[hpc_user_col])) and \
         (password_col not in data or pd.isnull(data[password_col])):
        raise ValueError('Missing required field password for non-hpc user {0}'.\
                         format(data[user_col]))
      
      username=data[user_col]
      hpc_username=data[hpc_user_col]
      password=data[password_col]
      
      check_cmd1=['iadmin','lu']
      check_cmd2=['grep','-w',quote(username)]
      c_proc1=subprocess.Popen(check_cmd1,stdout=subprocess.PIPE)
      c_proc2=subprocess.Popen(check_cmd2,stdin=c_proc1.stdout,stdout=subprocess.PIPE)
      c_proc1.stdout.close()
      result=c_proc2.communicate()[0]
      result=result.decode('UTF-8').split('\n')
      if len(result) == 1:
        raise ValueError('IRODS account exists for user {0}'.format(username))
      
      if len(result)>1:
        raise ValueError('Failed to correctly identify existing irods user for {0}'.\
                         format(username))
      irods_mkuser_cmd=['iadmin', 'mkuser', \
                        '{0}#igfZone'.format(quote(username)), 'rodsuser']
      subprocess.check_call(irods_mkuser_cmd)                                   # create irods user
      irods_chmod_cmd=['ichmod', '-M', 'own', 'igf', \
                       '/igfZone/home/{0}'.format(quote(username))]
      subprocess.check_call(irods_chmod_cmd)                                    # change permission for irods user
      irods_inherit_cmd=['ichmod','-r', 'inherit', \
                         '/igfZone/home/{0}'.format(quote(username))]
      subprocess.check_call(irods_inherit_cmd)                                  # inherit irods user
      
      if (hpc_username is None or hpc_username == '' ) and password:
        irods_passwd_cmd=['iadmin', 'moduser', \
                          '{0}#igfZone'.format(quote(username)), \
                          'password', ''.format(quote(password))]
        subprocess.check_call(irods_passwd_cmd)                                 # set password for non-hpc user
        
    except:
      raise


  def _get_hpc_username(self,username):
    '''
    An internal method for checking hpc accounts for new users
    required params:
    username: A username string
    '''
    try:
      cmd1=['ssh', \
           '{0}@{1}'.format(quote(self.hpc_user),quote(self.hpc_address)), \
           '"ldapsearch -x -h {0}"'.format(quote(self.ldap_server)), \
          ]
      cmd2=['grep',\
            '-w',\
            '"uid: {0}"'.format(quote(username)), \
           ]
      proc1=subprocess.Popen(cmd1,stdout=subprocess.PIPE)
      proc2=subprocess.Popen(cmd2,stdin=proc1.stdout,stdout=subprocess.PIPE)
      proc1.stdout.close()
      result=proc2.communicate()[0]
      result=result.decode('UTF-8').split('\n')
      if len(result)>0:
        hpc_user=username
      else:
        hpc_user=None
      return hpc_user
    except:
      raise
    
    
  def _assign_username_and_password(self,data,user_col='username',\
                                    hpc_user_col='hpc_username',\
                                    password_col='password',\
                                    email_col='email_id'):
    '''
    An internal method for assigning new user account and password
    required params:
    data: A pandas series containing user data
    user_col: Column name for username, deffault username
    password_col: Column name for password, default password
    hpc_user_col: Column name for hpc_username, default hpc_username
    email_id_col: Column name for email id, default email_id
    '''
    try:
      if not isinstance(data, pd.Series):
        raise ValueError('Expecting a pandas series and got {0}'.\
                         format(type(data)))
        
      if user_col not in data or pd.isnull(data[user_col]):                     # assign username from email id
        username,_=data[email_col].split('@',1)                                 # get username from email id
        data[user_col]=username[:10] if len(username)>10 \
                                     else username                              # allowing only first 10 chars of the email id
                                     
      if user_col in data and not pd.isnull(data[user_col]) and \
         hpc_user_col in data and not pd.isnull(data[hpc_user_col]) and \
         data[user_col] != data[hpc_user_col]:                                  # if user name and hpc username both are present, they should be same
        raise ValueError('username {0} and hpc_username {1} should be same'.\
                         format(data[user_col],data[hpc_user_col]))
        
      if (hpc_user_col not in data or \
          (hpc_user_col in data and pd.isnull(data[hpc_user_col]))) \
         and self.check_hpc_user:                                               # assign hpc username
        hpc_username=self._get_hpc_username(username=data[user_col])
        data[hpc_user_col]=hpc_username                                         # set hpc username
      
      if hpc_user_col not in data or \
         (hpc_user_col in data and pd.isnull(data[hpc_user_col])):
        if password_col not in data or \
           (password_col in data and pd.isnull(data[password_col])):
          data[password_col]=self._get_user_password()                          # assign a random password if its not supplied
          
      return data
    except:
      raise
  
  
  def _check_and_register_data(self,data):
    '''
    An internal method for checking and registering data
    required params:
    data: A dictionary containing following keys
          project_data
          user_data
          project_user_data
          sample_data
    '''
    try:
      db_connected=False
      project_data=data['project_data']
      user_data=data['user_data']
      project_user_data=data['project_user_data']
      sample_data=data['sample_data']
      user_data=user_data.apply(lambda x: \
                                self._assign_username_and_password(x), \
                                axis=1)                                         # check for use account and password
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
        pa1=ProjectAdaptor(**{'session':base.session})                          # connect to project adaptor
        pa1.store_project_and_attribute_data(data=project_data,autosave=False)  # load project data
      
      if len(user_data.index) > 0:                                              # store new users
        ua=UserAdaptor(**{'session':base.session})
        ua.store_user_data(data=user_data,autosave=False)                       # load user data
      
      if len(project_user_data.index) > 0:                                      # store new project users
        pa2=ProjectAdaptor(**{'session':base.session})                          # connect to project adaptor
        project_user_data=project_user_data.to_dict(orient='records')           # convert dataframe to dictionary
        pa2.assign_user_to_project(data=project_user_data, autosave=False)      # load project user data
      
      if len(sample_data.index) > 0:                                            # store new samples
        sa=SampleAdaptor(**{'session':base.session})                            # connect to sample adaptor
        sa.store_sample_and_attribute_data(data=sample_data,autosave=False)     # load samples data
      
      if self.setup_irods:
        user_data.apply(lambda x: self._setup_irods_account(data))              # create irods account
        
    except:
      if db_connected:
        base.rollback_session()                                                 # rollback session
      raise
    else:
      if db_connected:
        base.commit_session()                                                   # commit changes to db
        if len(user_data.index) > 0 and self.notify_user:
          user_data.apply(lambda x: self._notify_about_new_user_account(x),\
                          axis=1)                                               # send mail to new user with their password and forget it
    finally:
      if db_connected:
        base.close_session()                                                    # close db connection
  

  def _read_project_info_and_get_new_entries(self,project_info_file):
    '''
    An internal method for processing project info data
    required params:
    project_info_file: A filepath for project_info csv files
    
    It returns a dictionary with following keys
          project_data
          user_data
          project_user_data
          sample_data
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
      if self.project_lookup_column not in project_data.columns:
        raise ValueError('Missing required column: {0}'.\
                         format(self.project_lookup_column))
      if self.user_lookup_column not in user_data.columns:
        raise ValueError('Missing required column: {0}'.\
                         format(self.user_lookup_column))
      if self.sample_lookup_column not in sample_data.columns:
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
          if fnmatch.fnmatch(file_path, '*.csv'):                               # only consider csv files
            file_check=fa.check_file_records_file_path(file_path=file_path)     # check for file in db
            if not file_check:
              new_project_info_list.append(os.path.join(root_path,file_path))   # collect new project info files
      fa.close_session()                                                        # disconnect db
      return new_project_info_list
    except:
      raise
