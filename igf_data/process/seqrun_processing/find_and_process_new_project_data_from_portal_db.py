import re
from telnetlib import EL
import pandas as pd
from shlex import quote
from copy import deepcopy
import os, subprocess, fnmatch, warnings, string
from igf_data.utils.dbutils import read_dbconf_json, read_json_data
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.task_tracking.igf_slack import IGF_slack
from igf_data.igfdb.igfTables import Project, User
from igf_data.igfdb.useradaptor import UserAdaptor
from igf_data.utils.fileutils import get_temp_dir, remove_dir
from jinja2 import Environment, FileSystemLoader, select_autoescape
from igf_portal.api_utils import get_data_from_portal
from io import StringIO


class Find_and_register_new_project_data_from_portal_db:
  def __init__(
      self,
      dbconfig,
      user_account_template,
      portal_db_conf_file,
      log_slack=True,
      slack_config=None,
      check_hpc_user=False,
      hpc_user=None,
      hpc_address=None,
      ldap_server=None,
      setup_irods=True,
      notify_user=True,
      default_user_email='igf@imperial.ac.uk',
      project_lookup_column='project_igf_id',
      user_lookup_column='email_id',
      data_authority_column='data_authority',
      sample_lookup_column='sample_igf_id',
      barcode_check_keyword='barcode_check',
      metadata_sheet_name='Project metadata',
      sendmail_exe='/usr/sbin/sendmail'):
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
        self.sendmail_exe = sendmail_exe
        if log_slack and slack_config is None:
          raise ValueError('Missing slack config file')
        elif log_slack and slack_config:
          self.igf_slack = IGF_slack(slack_config=slack_config)
        if check_hpc_user and \
           (hpc_user is None or \
           hpc_address is None or \
           ldap_server is None):
          raise ValueError(
            'Hpc user {0} address {1}, and ldap server {2} are required for check_hpc_user'.\
              format(hpc_user,hpc_address,ldap_server))
      except Exception as e:
        raise ValueError('Init failed, error: {0}'.format(e))


  def process_project_data_and_account(self):
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


  def _compare_and_mark_portal_data_as_synced(self, project_data_dict):
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
      raise ValueError(e)


  def _get_new_project_data_from_portal(self):
    try:
      res = \
        get_data_from_portal(
          url_suffix="/api/v1/raw_metadata/download_ready_metadata",
          portal_config_file=self.portal_db_conf_file)
      return res
    except Exception as e:
      raise ValueError(e)


  def _mark_portal_db_as_synced(self):
    try:
      res = \
        get_data_from_portal(
          url_suffix="/api/v1/raw_metadata/mark_ready_metadata_as_synced",
          portal_config_file=self.portal_db_conf_file)
      return res
    except Exception as e:
      raise ValueError(e)


  def _read_project_info_and_get_new_entries(self, project_info_data):
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
      if self.project_lookup_column not in project_data.columns:
        raise ValueError(
                'Missing required column: {0}'.\
                  format(self.project_lookup_column))
      if self.user_lookup_column not in user_data.columns:
        raise ValueError(
                'Missing required column: {0}'.\
                  format(self.user_lookup_column))
      if self.sample_lookup_column not in sample_data.columns:
        raise ValueError(
                'Missing required column: {0}'.\
                  format(self.sample_lookup_column))                            # check if required columns are present in the dataframe
      output_data =  {
        'project_data': project_data,
        'user_data': user_data,
        'project_user_data': project_user_data,
        'sample_data': sample_data}
      return output_data
    except Exception as e:
      raise ValueError('Failed to read project info, error: {0}'.format(e))


  def _check_and_add_project_attributes(self, data_series):
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
              'Failed to add roject attribute, error: {0}'.format(e))


  def _check_and_register_data(self, data):
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
              'Failed to check and register data, error: {0}'.format(e))
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
      self, data, dbsession, table_name, check_column='EXISTS'):
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
                'Expecting a data series and got {0}'.\
                  format(type(data)))
      if table_name=='project':
        if self.project_lookup_column in data and \
           not pd.isnull(data[self.project_lookup_column]):
          project_igf_id = data[self.project_lookup_column]
          pa = ProjectAdaptor(**{'session':dbsession})                          # connect to project adaptor
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
                  'Missing or empty required column {0}'.\
                    format(self.project_lookup_column))
      elif table_name=='user':
        if self.user_lookup_column in data and \
           not pd.isnull(data[self.user_lookup_column]):
          user_email = data[self.user_lookup_column]
          ua = UserAdaptor(**{'session':dbsession})                             # connect to user adaptor
          user_exists = \
            ua.check_user_records_email_id(email_id=user_email)
          if user_exists:                                                       # store data only if user is not existing
            data[check_column] = True
          else:
            data[check_column] = False
          return data
        else:
          raise ValueError(
                  'Missing or empty required column {0}'.\
                    format(self.user_lookup_column))
      elif table_name=='sample':
        if self.sample_lookup_column in data and \
           not pd.isnull(data[self.sample_lookup_column]):
          project_igf_id = data[self.project_lookup_column]
          sample_igf_id = data[self.sample_lookup_column]
          sa = SampleAdaptor(**{'session':dbsession})                           # connect to sample adaptor
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
                      'Sample {0} exists in database but not associated with project {1}'.\
                        format(sample_igf_id,project_igf_id))                   # inconsistency in sample project combination
            data[check_column] = False
          return data
        else:
          raise ValueError(
                  'Missing or empty required column {0}'.\
                    format(self.sample_lookup_column))
      elif table_name=='project_user':
        if self.user_lookup_column in data and \
            not pd.isnull(data[self.user_lookup_column]) and \
           self.project_lookup_column in data and \
            not pd.isnull(data[self.project_lookup_column]):
          project_igf_id = data[self.project_lookup_column]
          user_email = data[self.user_lookup_column]
          pa = ProjectAdaptor(**{'session':dbsession})                          # connect to project adaptor
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
                  'Missing or empty required column {0}, {1}'.\
                  format(self.project_lookup_column,\
                         self.user_lookup_column))
      else:
        raise ValueError(
                'table {0} not supported'.format(table_name))
    except Exception as e:
      raise ValueError(
              'Failed to check existing data, error: {0}'.format(e))


  def _notify_about_new_user_account(
        self, data, user_col='username',
        password_col='password', hpc_user_col='hpc_username',
        name_col='name',email_id_col='email_id'):
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
        raise ValueError('Expecting a pandas series and got {0}'.\
                         format(type(data)))
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
            autoescape=select_autoescape(['html','xml']))                       # set template env
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
            fullName=fullname,
            userName=username,
            userPass=password).\
          dump(report_output_file)
        read_cmd = ['cat', quote(report_output_file)]
        proc = \
          subprocess.Popen(read_cmd, stdout=subprocess.PIPE)
        sendmail_cmd = [self.sendmail_exe, '-t']
        subprocess.check_call(
          sendmail_cmd,
          stdin=proc.stdout)
        proc.stdout.close()
        if proc.returncode !=None:
          raise ValueError(
                  'Failed running command {0}:{1}'.\
                  format(read_cmd,proc.returncode))
        remove_dir(temp_work_dir)
    except Exception as e:
      raise ValueError(
              'Failed to notify new user, error: {0}'.format(e))


  def _assign_username_and_password(
        self, data, user_col='username', hpc_user_col='hpc_username', password_col='password',
        email_col='email_id', hpc_category='HPC_USER', category_col='category'):
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
                'Expecting a pandas series and got {0}'.\
                  format(type(data)))
      if (user_col not in data or pd.isnull(data[user_col])) and \
         (hpc_user_col in data and not pd.isnull(data[hpc_user_col])):          # if hpc username found, make it username
        data[user_col] = data[hpc_user_col]
      if (user_col not in data or \
          (user_col in data and pd.isnull(data[user_col]))):                    # assign username from email id
        username, _ = data[email_col].split('@',1)                              # get username from email id
        data[user_col] = \
          username[:10] \
            if len(username)>10 \
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
                'username {0} and hpc_username {1} should be same'.\
                  format(data[user_col],data[hpc_user_col]))
      if (hpc_user_col not in data or pd.isnull(data[hpc_user_col])) and \
         (password_col not in data or pd.isnull(data[password_col])):
          data[password_col] = self._get_user_password()                        # assign a random password if its not supplied
      if (category_col not in data or pd.isnull(data[category_col])) and \
         (hpc_user_col in data and not pd.isnull(data[hpc_user_col])):          # set user category for hpc users
         data[category_col] = hpc_category
      return data
    except Exception as e:
      raise ValueError(
              'Failed to assign username and pass, error: {0}'.format(e))


  def _add_default_user_to_project(self, project_user_data):
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
      raise ValueError('Failed to default user, error: {0}'.format(e))


  def _get_hpc_username(self, username):
    '''
    An internal method for checking hpc accounts for new users
    This method is not reliable as the ldap server can be down from time to time

    :param username: A username string
    '''
    try:
      cmd1 = [
        'ssh',
        '{0}@{1}'.format(quote(self.hpc_user),
        quote(self.hpc_address)),
        'ldapsearch -x -h {0}'.format(quote(self.ldap_server))]
      cmd2 = [
        'grep',
        '-w',
        'uid: {0}'.format(quote(username))]
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
      if proc1.returncode !=None:
          raise ValueError(
                  'Failed running command {0}:{1}'.\
                  format(cmd1,proc1.returncode))
      result = proc2.communicate()[0]
      result = result.decode('UTF-8')
      if result=='':
        hpc_username = None
      else:
        hpc_username = username
      return hpc_username
    except Exception as e:
      raise ValueError('Failed to get hpc user name, error: {0}'.format(e))


  def _setup_irods_account(
        self, data, user_col='username', 
        password_col='password', hpc_user_col='hpc_username'):
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
                'Expecting a pandas series and got {0}'.\
                  format(type(data)))
      if user_col not in data or pd.isnull(data[user_col]):
        raise ValueError('Missing required username')
      if (hpc_user_col not in data or pd.isnull(data[hpc_user_col])) and \
         (password_col not in data or pd.isnull(data[password_col])):
        raise ValueError(
                'Missing required field password for non-hpc user {0}'.\
                  format(data[user_col]))
      username = data[user_col]
      hpc_username = data[hpc_user_col]
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
                  'Failed running command {0}:{1}'.\
                    format(check_cmd1,c_proc1.returncode))
      result = c_proc2.communicate()[0]
      result = result.decode('UTF-8')
      if result != '' and pd.isnull(data[hpc_user_col]):                        # for non hpc users
        if self.check_hpc_user:
          raise ValueError(
                  'Can not reset iRODS password for non hpc user {0} with check_hpc_user option'.\
                    format(username))
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
                'resetting irods account password for non-hpc user: {0}, password length: {1}'.\
                  format(username,len(password))
              self.igf_slack.post_message_to_channel(
                message,reaction='pass')
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
      raise ValueError('Failed to setup irods account, error: {0}'.format(e))

  @staticmethod
  def _get_user_password(password_length=12):
    '''
    An internal staticmethod for generating random password

    :param password_length: Required length of password, default 12
    '''
    try:
      new_password = None                                                       # default value of the new password is None
      symbols = '^!'                                                            # allowed symbols in password
      chars = string.ascii_lowercase+\
              string.ascii_uppercase+\
              string.digits+\
              symbols                                                           # a string of lower case and upper case letters, digits and symbols
      symbol_pattern = re.compile(r'^[{0}]'.format(string.punctuation))
      digit_pattern = re.compile(r'^[0-9]+')
      while new_password is None or \
            re.match(symbol_pattern,new_password) or \
            re.match(digit_pattern,new_password):                               # password can't be None or starts with digit or a symbol
        new_password = \
          ''.join([chars[ord(os.urandom(1)) % len(chars)] \
                          for i in range(password_length)])                     # assign a new random password
      return new_password
    except Exception as e:
      raise ValueError(
              'Failed to create random password, error: {0}'.format(e))
