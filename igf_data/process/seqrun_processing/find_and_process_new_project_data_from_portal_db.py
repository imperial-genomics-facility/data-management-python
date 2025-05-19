import re
import time
import pandas as pd
from shlex import quote
from copy import deepcopy
from typing import Tuple
from typing import Union
from typing import Any
import os
import subprocess
import string
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.utils.dbutils import  read_json_data
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.task_tracking.igf_slack import IGF_slack
from igf_data.igfdb.igfTables import Project, User
from igf_data.igfdb.useradaptor import UserAdaptor
from igf_data.utils.fileutils import get_temp_dir
from igf_data.utils.fileutils import remove_dir
from igf_data.utils.fileutils import check_file_path
from jinja2 import Environment
from jinja2 import FileSystemLoader
from jinja2 import select_autoescape
from igf_portal.api_utils import get_data_from_portal
from io import StringIO
import smtplib


def send_email_via_smtp(
      sender: str,
      receivers: list,
      email_config_json: str,
      email_text_file: str,
      host_key: str = 'host',
      port_key: str = 'port',
      user_key: str = 'username',
      pass_key: str = 'password') \
        -> None:
  try:
    check_file_path(email_text_file)
    with open(email_text_file, 'r') as fp:
      email_message = fp.readlines()
      if len(email_message) == 0:
        raise ValueError(
          f"Failed to send mail. No txt found in {email_text_file}")
      email_message = ''.join(email_message)
      email_config = \
        read_json_data(email_config_json)
      if isinstance(email_config, list):
        email_config = email_config[0]
      if host_key not in email_config or \
         port_key not in email_config or \
         user_key not in email_config or \
         pass_key not in email_config:
        raise KeyError("Missing email config key")
      smtpObj = \
        smtplib.SMTP(email_config.get(host_key), email_config.get(port_key))
      smtpObj.ehlo()
      smtpObj.starttls()
      smtpObj.ehlo()
      smtpObj.login(
        email_config.get(user_key),
        email_config.get(pass_key))
      smtpObj.sendmail(
        sender, receivers, email_message)
      time.sleep(10)
      smtpObj.quit()
  except Exception as e:
    raise ValueError(f"Failed to send mail. Error {e}")




class Find_and_register_new_project_data_from_portal_db:
  def __init__(
      self,
      dbconfig: str,
      user_account_template: str,
      portal_db_conf_file: str,
      log_slack: bool = True,
      slack_config: Union[str, None] = None,
      check_hpc_user: bool = False,
      hpc_user: Union[str, None] = None,
      hpc_address: Union[str, None] = None,
      ldap_server: Union[str, None] = None,
      setup_irods: Union[bool, None] = True,
      notify_user: Union[bool, None] = True,
      default_user_email: str = '',
      project_lookup_column: str = 'project_igf_id',
      user_lookup_column: str = 'email_id',
      data_authority_column: str = 'data_authority',
      sample_lookup_column: str = 'sample_igf_id',
      barcode_check_keyword: str = 'barcode_check',
      metadata_sheet_name: str = 'Project metadata',
      email_config_json: Union[str, None] = None) \
        -> None:
      try:
        self.user_account_template = user_account_template
        self.portal_db_conf_file = portal_db_conf_file
        self.project_lookup_column = project_lookup_column
        self.user_lookup_column = user_lookup_column
        self.sample_lookup_column = sample_lookup_column
        self.data_authority_column = data_authority_column
        self.log_slack = log_slack
        dbparams = read_dbconf_json(dbconfig)
        base = BaseAdaptor(**dbparams)
        self.session_class = base.get_session_class()
        self.setup_irods=setup_irods
        self.notify_user = notify_user
        self.default_user_email = default_user_email
        self.barcode_check_keyword = barcode_check_keyword
        self.check_hpc_user = check_hpc_user
        self.hpc_user = hpc_user
        self.hpc_address = hpc_address
        self.ldap_server = ldap_server
        self.metadata_sheet_name = metadata_sheet_name
        self.email_config_json = email_config_json
        if log_slack and slack_config is None:
          raise ValueError('Missing slack config file')
        elif log_slack and slack_config:
          self.igf_slack = IGF_slack(slack_config=slack_config)
        if check_hpc_user and \
           (hpc_user is None or \
           hpc_address is None or \
           ldap_server is None):
          raise ValueError(
            f'Hpc user {hpc_user} address {hpc_address}, and ldap server {ldap_server} needed for hpc check')
      except Exception as e:
        raise ValueError(
          f'Init failed, error: {e}')


  def process_project_data_and_account(self) \
        -> None:
    try:
      new_project_data_dict = \
        self._get_new_project_data_from_portal()
      for _, project_info_data in new_project_data_dict.items():
        formatted_data = \
          self._read_project_info_and_get_new_entries(
            project_info_data=project_info_data)
        self._check_and_register_data(
          data=formatted_data)
      self._compare_and_mark_portal_data_as_synced(
        project_data_dict=new_project_data_dict)
    except Exception as e:
      raise ValueError(e)


  def _compare_and_mark_portal_data_as_synced(
        self,
        project_data_dict: dict) \
          -> None:
    try:
      mark_portal_db = True
      new_project_data_dict = \
        self._get_new_project_data_from_portal()
      for project_name in new_project_data_dict.keys():
        if project_name not in project_data_dict:
          mark_portal_db = False                                                # don't mark db recrds as done, if new entries are present
      if mark_portal_db:
        self._mark_portal_db_as_synced()
    except Exception as e:
      raise ValueError(
        f"Failed to change status as sycn in portal, error: {e}")


  def _get_new_project_data_from_portal(self) \
        -> Any:
    try:
      res = \
        get_data_from_portal(
          url_suffix="/api/v1/raw_metadata/download_ready_metadata",
          portal_config_file=self.portal_db_conf_file)
      return res
    except Exception as e:
      raise ValueError(
        f"Failed to get metadata from portal, error: {e}")


  def _mark_portal_db_as_synced(self) \
        -> Any:
    try:
      res = \
        get_data_from_portal(
          url_suffix="/api/v1/raw_metadata/mark_ready_metadata_as_synced",
          portal_config_file=self.portal_db_conf_file)
      return res
    except Exception as e:
      raise ValueError(e)


  def _read_project_info_and_get_new_entries(
        self,
        project_info_data: Union[list, str]) \
          -> dict:
    '''
    An internal method for processing project info data

    :param project_info_data: A list of dictionary for project_info csv data
    :returns: A dictionary with following keys

      * project_data
      * user_data
      * project_user_data
      * sample_data
    '''
    try:
      if isinstance(project_info_data, list):
        project_info_data = pd.DataFrame(project_info_data)
      if isinstance(project_info_data, str):
        csvStringIO = StringIO(project_info_data)
        project_info_data = pd.read_csv(csvStringIO, header=0)
      base = \
        BaseAdaptor(**{'session_class':self.session_class})
      required_project_columns = \
        base.get_table_columns(
          table_name=Project,
          excluded_columns=['project_id'])                                      # get project columns
      required_project_columns.\
        append(self.barcode_check_keyword)                                      # add barcode check param to project attribute table
      required_user_columns = \
        base.get_table_columns(
          table_name=User,
          excluded_columns=['user_id'])                                         # get user columns
      required_project_user_columns = [
        'project_igf_id',
        'email_id']                                                             # get project user columns
      project_data_columns = \
        project_info_data.columns.intersection(
          required_project_columns)
      project_data = \
        project_info_data.\
          loc[:, project_data_columns]                                          # get data for project table
      user_data_columns = \
        project_info_data.columns.intersection(
          required_user_columns)
      user_data = \
        project_info_data.loc[:, user_data_columns]                             # get data for user table
      project_user_data_columns = \
        project_info_data.columns.intersection(
          required_project_user_columns)
      project_user_data = \
        project_info_data.loc[:, project_user_data_columns]                     # get data for project user table
      required_sample_columns = \
        list(set(project_info_data.columns).\
                difference(set(list(project_data)+\
                               list(user_data)+\
                               list(project_user_data))))                       # all remaining column goes to sample tables
      required_sample_columns.\
        append('project_igf_id')
      sample_data = \
        project_info_data.loc[:, required_sample_columns]                       # get data for sample table
      project_data = \
        project_data.drop_duplicates()
      project_data = \
        project_data.apply(
          lambda x: \
            self._check_and_add_project_attributes(x),
          axis=1)                                                               # add missing project attribute to the dataframe
      project_data['project_igf_id'] = \
        project_data['project_igf_id'].\
          map(lambda x: x.replace(' ', ''))                                     # replace any white space from project igf id
      user_data = user_data.drop_duplicates()
      user_data['email_id'] = \
        user_data['email_id'].\
          map(lambda x: x.replace(' ', ''))                                     # replace any white space from email id
      if 'name' in user_data.columns:
        user_data['name'].\
          fillna('',inplace=True)
        user_data['name'] = \
          user_data['name'].\
            map(lambda x: x.title())                                            # reformat name, if its present
      project_user_data = \
        project_user_data.drop_duplicates()
      project_user_data['project_igf_id'] = \
        project_user_data['project_igf_id'].\
          map(lambda x: x.replace(' ',''))                                      # replace any white space from project igf id
      project_user_data['email_id'] = \
        project_user_data['email_id'].\
          map(lambda x: x.replace(' ', ''))                                     # replace any white space from email id
      sample_data = sample_data.drop_duplicates()                               # remove duplicate entries
      sample_data['project_igf_id'] = \
        sample_data['project_igf_id'].\
          map(lambda x: x.replace(' ',''))                                      # replace any white space from project igf id
      sample_data['sample_igf_id'] = \
        sample_data['sample_igf_id'].\
          map(lambda x: x.replace(' ',''))                                      # replace any white space from sample igf id
      if 'taxon_id' in sample_data.columns:
        sample_data['taxon_id'] = \
          sample_data['taxon_id'].\
            astype(str).str.replace('UNKNOWN', '0').\
            fillna('0')                                                         # replace any unknown taxon id with 0
        sample_data['taxon_id'] = \
          sample_data['taxon_id'].\
            astype(int)
      else:
        sample_data['taxon_id'] = 0
      if self.project_lookup_column not in project_data.columns:
        raise ValueError(
          f'Missing required column: {self.project_lookup_column}')
      if self.user_lookup_column not in user_data.columns:
        raise ValueError(
          f'Missing required column: {self.user_lookup_column}')
      if self.sample_lookup_column not in sample_data.columns:
        raise ValueError(
          f'Missing required column: {self.sample_lookup_column}')                            # check if required columns are present in the dataframe
      output_data =  {
        'project_data': project_data,
        'user_data': user_data,
        'project_user_data': project_user_data,
        'sample_data': sample_data}
      return output_data
    except Exception as e:
      raise ValueError(
        f'Failed to read project info, error: {e}')


  def _check_and_add_project_attributes(
        self,
        data_series: pd.Series) \
          -> pd.Series:
    '''
    An internal method for checking project data and adding required attributes

    :param data_series: A Pandas Series containing project data
    :returns: A Pandas series with project attribute information
    '''
    try:
      if not isinstance(data_series, pd.Series):
        raise AttributeError(
                'Expecting a Pandas Series and got {0}'.\
                  format(type(data_series)))
      if self.barcode_check_keyword not in data_series or  \
         pd.isnull(data_series[self.barcode_check_keyword]):
        data_series[self.barcode_check_keyword] = 'ON'                          # by default barcode checking is always ON
      return data_series
    except Exception as e:
      raise ValueError(
        f'Failed to add roject attribute, error: {e}')


  def _check_and_register_data(
        self,
        data: dict) \
          -> None:
    try:
      db_connected = False
      project_data = data['project_data']
      user_data = data['user_data']
      project_user_data = data['project_user_data']
      sample_data = data['sample_data']
      base = BaseAdaptor(**{'session_class':self.session_class})
      base.start_session()                                                      # connect_to db
      db_connected = True
      project_data = \
        project_data[project_data[self.project_lookup_column].isnull()==False]
      project_data = project_data.drop_duplicates()
      if project_data.index.size > 0:
        project_data['EXISTS'] = ''
        project_data = \
          project_data.apply(
            lambda x: \
              self._check_existing_data(
                data=x,
                dbsession=base.session,
                table_name='project',
                check_column='EXISTS'),
            axis=1,
            result_type=None)
        project_data = \
          project_data[project_data['EXISTS']==False]                           # filter existing projects
        project_data.drop(
          'EXISTS',
          axis=1,
          inplace=True)                                                         # remove extra column
      user_data = \
        user_data[user_data[self.user_lookup_column].isnull()==False]
      user_data = user_data.drop_duplicates()
      if user_data.index.size > 0:
        user_data = \
          user_data.apply(
            lambda x: \
              self._assign_username_and_password(x),
            axis=1)                                                             # check for use account and password
        user_data['EXISTS'] = ''
        user_data = \
          user_data.apply(
            lambda x: \
              self._check_existing_data(
                data=x,
                dbsession=base.session,
                table_name='user',
                check_column='EXISTS'),\
            axis=1,
            result_type=None)                                                   # get user map
        user_data = \
          user_data[user_data['EXISTS']==False]                                 # filter existing users
        user_data.drop(
          'EXISTS',
          axis=1,
          inplace=True)                                                         # remove extra column
      sample_data = \
        sample_data[sample_data[self.sample_lookup_column].isnull()==False]
      sample_data = \
        sample_data.drop_duplicates()
      if sample_data.index.size > 0:
        sample_data['EXISTS'] = ''
        sample_data = \
          sample_data.apply(
            lambda x: \
              self._check_existing_data(
                data=x,
                dbsession=base.session,
                table_name='sample',
                check_column='EXISTS'),\
            axis=1,
            result_type=None)                                                   # get sample map
        sample_data = \
          sample_data[sample_data['EXISTS']==False]                             # filter existing samples
        sample_data.drop(
          'EXISTS',
          axis=1,
          inplace=True)                                                         # remove extra column
      project_user_data = \
        project_user_data.drop_duplicates()
      project_user_data_mask = \
        (project_user_data[self.project_lookup_column].isnull()==False) & \
        (project_user_data[self.user_lookup_column].isnull()==False)
      project_user_data = \
        project_user_data[project_user_data_mask]                               # not allowing any empty values for project or user lookup
      if project_user_data.index.size > 0:
        project_user_data = \
          self._add_default_user_to_project(project_user_data)                  # update project_user_data with default users
        project_user_data['EXISTS'] = ''
        project_user_data[self.data_authority_column] = ''
        project_user_data = \
          project_user_data.apply(
            lambda x: \
              self._check_existing_data(
                data=x,
                dbsession=base.session,
                table_name='project_user',
                check_column='EXISTS'),\
            axis=1,
            result_type=None)                                                   # get project user map
        project_user_data = \
          project_user_data[project_user_data['EXISTS']==False]                 # filter existing project user
        project_user_data.drop(
          'EXISTS',
          axis=1,
          inplace=True)                                                         # remove extra column
        if len(project_data.index) > 0:                                         # store new projects
          pa1 = \
            ProjectAdaptor(**{'session':base.session})                          # connect to project adaptor
          pa1.store_project_and_attribute_data(
            data=project_data,
            autosave=False)                                                     # load project data
        if len(user_data.index) > 0:                                            # store new users
          ua = UserAdaptor(**{'session':base.session})
          ua.store_user_data(
            data=user_data,
            autosave=False)                                                     # load user data
        if len(project_user_data.index) > 0:                                    # store new project users
          pa2 = ProjectAdaptor(**{'session':base.session})                      # connect to project adaptor
          project_user_data = \
            project_user_data.to_dict(orient='records')                         # convert dataframe to dictionary
          pa2.assign_user_to_project(
            data=project_user_data,
            autosave=False)                                                     # load project user data
        if len(sample_data.index) > 0:                                          # store new samples
          sa = SampleAdaptor(**{'session':base.session})                        # connect to sample adaptor
          sa.store_sample_and_attribute_data(
            data=sample_data,
            autosave=False)                                                     # load samples data
        if self.setup_irods:
          user_data.apply(
            lambda x: \
              self._setup_irods_account(data=x),
            axis=1)                                                             # create irods account
    except Exception as e:
      if db_connected:
        base.rollback_session()
      raise ValueError(
        f'Failed to check and register data, error: {e}')
    else:
      if db_connected:
        base.commit_session()                                                   # commit changes to db
        if len(user_data.index) > 0 and self.notify_user:
          user_data.apply(
            lambda x: \
              self._notify_about_new_user_account(x),
            axis=1)                                                             # send mail to new user with their password and forget it
    finally:
      if db_connected:
        base.close_session()                                                    # close db connection


  def _check_existing_data(
        self,
        data: pd.Series,
        dbsession: Any,
        table_name: str,
        check_column: str = 'EXISTS') \
          -> pd.Series:
    '''
    An internal function for checking and registering project info

    :param data: A pandas data series
    :param dbsession: A sqlalchemy database session object
    :param table_name: A database table name
    :param check_column: Column name for existing data
    '''
    try:
      if not isinstance(data, pd.Series):
        raise ValueError(
          f'Expecting a data series and got {type(data)}')
      if table_name=='project':
        if self.project_lookup_column in data and \
           not pd.isnull(data[self.project_lookup_column]):
          project_igf_id = data[self.project_lookup_column]
          pa = ProjectAdaptor(**{'session': dbsession})                          # connect to project adaptor
          project_exists = \
            pa.check_project_records_igf_id(
              project_igf_id)
          if project_exists:                                                    # store data only if project is not existing
            data[check_column] = True
          else:
            data[check_column] = False
          return data
        else:
          raise ValueError(
            f'Missing or empty required column {self.project_lookup_column}')
      elif table_name=='user':
        if self.user_lookup_column in data and \
           not pd.isnull(data[self.user_lookup_column]):
          user_email = data[self.user_lookup_column]
          ua = UserAdaptor(**{'session': dbsession})                             # connect to user adaptor
          user_exists = \
            ua.check_user_records_email_id(email_id=user_email)
          if user_exists:                                                       # store data only if user is not existing
            data[check_column] = True
          else:
            data[check_column] = False
          return data
        else:
          raise ValueError(
            f'Missing or empty required column {self.user_lookup_column}')
      elif table_name=='sample':
        if self.sample_lookup_column in data and \
           not pd.isnull(data[self.sample_lookup_column]):
          project_igf_id = data[self.project_lookup_column]
          sample_igf_id = data[self.sample_lookup_column]
          sa = SampleAdaptor(**{'session': dbsession})                           # connect to sample adaptor
          sample_project_exists = \
            sa.check_project_and_sample(
              project_igf_id=project_igf_id,
              sample_igf_id=sample_igf_id)                                      # check for existing sample_id and project-id combination
          if sample_project_exists:                                             # store data only if sample is not existing
            data[check_column] = True
          else:
            sample_exists = \
              sa.check_sample_records_igf_id(
                sample_igf_id)                                                  # check for existing sample
            if sample_exists:
              raise ValueError(
                f'Sample {sample_igf_id} exists in db but not linked with project {project_igf_id}')                   # inconsistency in sample project combination
            data[check_column] = False
          return data
        else:
          raise ValueError(
            f'Missing or empty required column {self.sample_lookup_column}')
      elif table_name=='project_user':
        if self.user_lookup_column in data and \
            not pd.isnull(data[self.user_lookup_column]) and \
           self.project_lookup_column in data and \
            not pd.isnull(data[self.project_lookup_column]):
          project_igf_id = data[self.project_lookup_column]
          user_email = data[self.user_lookup_column]
          pa = ProjectAdaptor(**{'session': dbsession})                          # connect to project adaptor
          project_user_exists = \
            pa.check_existing_project_user(
              project_igf_id,
              email_id=user_email)
          if user_email != self.default_user_email and \
             (self.data_authority_column not in data or \
             pd.isnull(data[self.data_authority_column]) or \
             data.get(self.data_authority_column)==''):
            if project_user_exists:
              data[self.data_authority_column] = ''                             # set user as data authority, filter default user
            else:
              data[self.data_authority_column] = True
          if project_user_exists:                                               # store data only if sample is not existing
            data[check_column] = True
          else:
            data[check_column] = False
          return data
        else:
          raise ValueError(
            f'Missing or empty required column {self.project_lookup_column}, {self.user_lookup_column}')
      else:
        raise ValueError(
          f'table {table_name} not supported')
    except Exception as e:
      raise ValueError(
        f'Failed to check existing data, error: {e}')


  def _notify_about_new_user_account(
        self,
        data: pd.Series,
        user_col: str = 'username',
        password_col: str = 'password',
        hpc_user_col: str = 'hpc_username',
        name_col: str = 'name',
        email_id_col: str = 'email_id') \
          -> None:
    '''
    An internal method for sending mail to new user with their password

    :param data: A pandas series containing user data
    :param user_col: Column name for username, default username
    :param password_col: Column name for password, default password
    :param hpc_user_col: Column name for hpc_username, default hpc_username
    :param name_col: Column name for name, default name
    :param email_id_col: Column name for email id, default email_id
    '''
    try:
      if not isinstance(data, pd.Series):
        raise ValueError(
          f'Expecting a pandas series and got {type(data)}')
      username = data[user_col]
      fullname = data[name_col]
      password = data[password_col]
      email_id = data[email_id_col]
      if hpc_user_col not in data or \
         pd.isnull(data[hpc_user_col]):                                         # send email only to non-hpc users
        template_dir = \
          os.path.dirname(self.user_account_template)
        template_env = \
          Environment(
            loader=\
              FileSystemLoader(searchpath=template_dir),
            autoescape=select_autoescape(['html', 'xml']))                       # set template env
        template_file = \
          template_env.\
            get_template(os.path.basename(self.user_account_template))
        temp_work_dir = get_temp_dir()                                          # get a temp dir
        report_output_file = \
          os.path.join(
            temp_work_dir,
            'email_template.txt')
        template_file.\
          stream(
            userEmail=email_id,
            default_user_email=self.default_user_email,
            fullName=fullname,
            userName=username,
            userPass=password).\
          dump(report_output_file)
        # read_cmd = ['cat', quote(report_output_file)]
        # proc = \
        #   subprocess.Popen(read_cmd, stdout=subprocess.PIPE)
        # sendmail_cmd = [self.sendmail_exe, '-t']
        # subprocess.check_call(
        #   sendmail_cmd,
        #   stdin=proc.stdout)
        # proc.stdout.close()
        # if proc.returncode !=None:
        #   raise ValueError(
        #           'Failed running command {0}:{1}'.\
        #           format(read_cmd,proc.returncode))
        if self.email_config_json is None:
          raise ValueError("Missing email config file")
        send_email_via_smtp(
          sender=self.default_user_email,
          receivers=[email_id, self.default_user_email],
          email_config_json=self.email_config_json,
          email_text_file=report_output_file)
        remove_dir(temp_work_dir)
    except Exception as e:
      raise ValueError(
        f'Failed to notify new user, error: {e}')


  def _assign_username_and_password(
        self,
        data: pd.Series,
        user_col: str = 'username',
        hpc_user_col: str = 'hpc_username',
        password_col: str = 'password',
        email_col: str = 'email_id',
        hpc_category: str = 'HPC_USER',
        category_col: str = 'category') \
          -> pd.Series:
    '''
    An internal method for assigning new user account and password

    :param data: A pandas series containing user data
    :param user_col: Column name for username, deffault username
    :param password_col: Column name for password, default password
    :param hpc_user_col: Column name for hpc_username, default hpc_username
    :param email_id_col: Column name for email id, default email_id
    :param category_col: Column name for user category, default category
    :param hpc_category: Category tag for hpc user, default: HPC_USER
    '''
    try:
      if not isinstance(data, pd.Series):
        raise ValueError(
          f'Expecting a pandas series and got {type(data)}')
      if (user_col not in data or pd.isnull(data[user_col])) and \
         (hpc_user_col in data and not pd.isnull(data[hpc_user_col])):          # if hpc username found, make it username
        data[user_col] = data[hpc_user_col]
      if (user_col not in data or \
          (user_col in data and pd.isnull(data[user_col]))):                    # assign username from email id
        username, _ = data[email_col].split('@', 1)                             # get username from email id
        data[user_col] = \
          username[: 10] \
            if len(username) > 10 \
              else username                                                     # allowing only first 10 chars of the email id
      #if (hpc_user_col not in data or pd.isnull(data[hpc_user_col])) and \
      #   self.check_hpc_user:                                                   # assign hpc username
      #  hpc_username = \
      #    self._get_hpc_username(
      #      username=data[user_col])
      #  data[hpc_user_col] = hpc_username                                       # set hpc username
      if user_col in data and not pd.isnull(data[user_col]) and \
         hpc_user_col in data and not pd.isnull(data[hpc_user_col]) and \
         data[user_col] != data[hpc_user_col]:                                  # if user name and hpc username both are present, they should be same
        raise ValueError(
          f'username {data[user_col]} and hpc_username {data[hpc_user_col]} should be same')
      if (hpc_user_col not in data or pd.isnull(data[hpc_user_col])) and \
         (password_col not in data or pd.isnull(data[password_col])):
          data[password_col] = self._get_user_password()                        # assign a random password if its not supplied
      if (category_col not in data or pd.isnull(data[category_col])) and \
         (hpc_user_col in data and not pd.isnull(data[hpc_user_col])):          # set user category for hpc users
         data[category_col] = hpc_category
      return data
    except Exception as e:
      raise ValueError(
        f'Failed to assign username and pass, error: {e}')


  def _add_default_user_to_project(
        self,
        project_user_data: pd.DataFrame) \
          -> pd.DataFrame:
    '''
    An internal method for adding default user to the project_user_data dataframe

    :param project_user_data: A dataframe containing project_igf_id and email_id column
    :returns: a pandas dataframe with new row for the project_igf_id and default_user_email
    '''
    try:
      new_project_user_data = list()
      for row in project_user_data.to_dict(orient='records'):
        new_project_user_data.append(row)
        row2 = deepcopy(row)
        row2[self.user_lookup_column] = self.default_user_email
        new_project_user_data.append(row2)
      new_project_user_data = pd.DataFrame(new_project_user_data)
      return new_project_user_data
    except Exception as e:
      raise ValueError(
        f'Failed to default user, error: {e}')


  def _get_hpc_username(
        self,
        username: str) \
          -> str:
    '''
    An internal method for checking hpc accounts for new users
    This method is not reliable as the ldap server can be down from time to time

    :param username: A username string
    '''
    try:
      hpc_user = quote(self.hpc_user)
      hpc_address = quote(self.hpc_address)
      ldap_server = quote(self.ldap_server)
      username = quote(username)
      cmd1 = [
        'ssh',
        f'{hpc_user}@{hpc_address}',
        f'ldapsearch -x -h {ldap_server}']
      cmd2 = [
        'grep',
        '-w',
        'uid: {username}']
      proc1 = \
        subprocess.Popen(
          cmd1,
          stdout=subprocess.PIPE)
      proc2 = \
        subprocess.Popen(
          cmd2,
          stdin=proc1.stdout,
          stdout=subprocess.PIPE)
      proc1.stdout.close()
      if proc1.returncode != None:
          raise ValueError(
            f'Failed running command {cmd1} : {proc1.returncode}')
      result = proc2.communicate()[0]
      result = result.decode('UTF-8')
      if result=='':
        hpc_username = None
      else:
        hpc_username = username
      return hpc_username
    except Exception as e:
      raise ValueError(
        f'Failed to get hpc user name, error: {e}')


  def _setup_irods_account(
        self,
        data: pd.Series,
        user_col: str = 'username',
        password_col: str = 'password',
        hpc_user_col: str = 'hpc_username') \
          -> None:
    '''
    An internal method for creating new user account in irods

    :param data: A pandas series containing user data
    :param user_col: Column name for username, deffault username
    :param password_col: Column name for password, default password
    :param hpc_user_col: Column name for hpc_username, default hpc_username
    '''
    try:
      if not isinstance(data, pd.Series):
        raise ValueError(
          f'Expecting a pandas series and got {type(data)}')
      if user_col not in data or pd.isnull(data[user_col]):
        raise ValueError('Missing required username')
      if (hpc_user_col not in data or pd.isnull(data[hpc_user_col])) and \
         (password_col not in data or pd.isnull(data[password_col])):
        raise ValueError(
          f'Missing required field password for non-hpc user {data[user_col]}')
      username = data[user_col]
      hpc_username = None # data[hpc_user_col]
      password = data[password_col]
      check_cmd1 = ['iadmin', 'lu']
      check_cmd2 = ['grep', '-w', quote(username)]
      c_proc1 = \
        subprocess.Popen(
          check_cmd1,
          stdout=subprocess.PIPE)
      c_proc2 = \
        subprocess.Popen(
          check_cmd2,
          stdin=c_proc1.stdout,
          stdout=subprocess.PIPE)
      c_proc1.stdout.close()
      if c_proc1.returncode != None:
          raise ValueError(
            f'Failed running command {check_cmd1}:{c_proc1.returncode}')
      result = c_proc2.communicate()[0]
      result = result.decode('UTF-8')
      if result != '' and hpc_username is None: #pd.isnull(data[hpc_user_col]):                        # for non hpc users
        if self.check_hpc_user:
          raise ValueError(
            f'Can not reset iRODS password for non hpc user {username} with check_hpc_user option')
        else:
          if password is not None or password != '':
            irods_passwd_cmd = \
              '{0} {1} {2}#{3} {4} {5}'.\
              format(
                'iadmin',
                'moduser',
                quote(username),
                'igfZone',
                'password',
                quote(password))                                                # format irods command for shell
            subprocess.check_call(
              irods_passwd_cmd,
              shell=True)
            if self.log_slack:
              message = \
                f'resetting irods account password for non-hpc user: {username}, password length: {len(password)}'
              self.igf_slack.post_message_to_channel(
                message, reaction='pass')
          else:
            raise ValueError(
                    'Missing password for non-hpc user {0}'.\
                      format(quote(username)))
      elif result=='':
        irods_mkuser_cmd = [
          'iadmin',
          'mkuser',
          '{0}#igfZone'.format(quote(username)),
          'rodsuser']
        subprocess.check_call(irods_mkuser_cmd)                                 # create irods user
        irods_chmod_cmd = [
          'ichmod',
          '-M',
          'own',
          'igf',
          '/igfZone/home/{0}'.format(quote(username))]
        subprocess.check_call(irods_chmod_cmd)                                  # change permission for irods user
        irods_inherit_cmd = [
          'ichmod',
          '-r',
          'inherit',
          '/igfZone/home/{0}'.format(quote(username))]
        subprocess.check_call(irods_inherit_cmd)                                # inherit irods user
        if (hpc_username is None or hpc_username == '' ) and \
           (password is not None or password != ''):
          if len(password)>20:
            raise ValueError(
                    'check password for non hpc user {0}: {1}'.\
                      format(username,password))                                # it could be the encrypted password
          irods_passwd_cmd = \
            '{0} {1} {2}#{3} {4} {5}'.\
            format(
              'iadmin',
              'moduser',
              quote(username),
              'igfZone',
              'password',
              quote(password))                                                  # format irods command for shell
          subprocess.check_call(
            irods_passwd_cmd,
            shell=True)                                                         # set password for non-hpc user
          if self.log_slack:
            message='created irods account for non-hpc user: {0}'.\
                    format(username)
            self.igf_slack.post_message_to_channel(
              message,reaction='pass')
    except Exception as e:
      raise ValueError(
        f'Failed to setup irods account, error: {e}')


  @staticmethod
  def _get_user_password(password_length: int = 12) -> str:
    '''
    An internal staticmethod for generating random password

    :param password_length: Required length of password, default 12
    '''
    try:
      new_password = None                                                       # default value of the new password is None
      symbols = '^!'                                                            # allowed symbols in password
      chars = \
        string.ascii_lowercase + \
        string.ascii_uppercase + \
        string.digits + \
        symbols                                                                 # a string of lower case and upper case letters, digits and symbols
      symbol_pattern = \
        re.compile(r'^[{string.punctuation}]')
      digit_pattern = \
        re.compile(r'^[0-9]+')
      while new_password is None or \
            re.match(symbol_pattern,new_password) or \
            re.match(digit_pattern,new_password):                               # password can't be None or starts with digit or a symbol
        new_password = \
          ''.join([chars[ord(os.urandom(1)) % len(chars)] \
                      for i in range(password_length)])                         # assign a new random password
      return new_password
    except Exception as e:
      raise ValueError(
        f'Failed to create random password, error: {e}')
