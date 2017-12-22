import pandas as pd
import json, hashlib, os, codecs, base64
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.igfTables import User

class UserAdaptor(BaseAdaptor):
  '''
  An adaptor class for table User
  '''
  def _email_check(self, email):
    '''
    An internal function to check if email_id has '@' or not
    require param:
    email: a string containing the email id
    '''
    if '@' not in email:
      raise ValueError('Email id {0} is not correctly formatted'.format(email))


  def _encrypt_password(self, series, password_column='password', salt_column='encryption_salt', ht_pass_column='ht_password'):
    '''
    An internal function for encrypting password
    '''
    if not isinstance(series, pd.Series):
      series=pd.DataFrame(series)

    if password_column in series.index and series[password_column]:             # password is optional
      salt=codecs.encode(os.urandom(32),"hex").decode("utf-8")                  # calculate salt value
      password=series[password_column]                                          # fetch password
      if not isinstance(password, str):
        password=str(series.password_column).encode('utf-8')                    # encode password if its not a string

      if password:                                                              # always encrypt password
        ht_pass='{0}{1}'.format('{SHA}',base64.b64encode(\
                                        hashlib.sha1(password.encode('utf-8')).\
                                        digest()).decode())                     # calculate sha1 for htaccess password
        series[ht_pass_column]=ht_pass                                          # set htaccess password
        key=salt+password                                                       # construct key using salt and password
        password=hashlib.sha512(str(key).encode('utf-8')).hexdigest()           # create password hash
        series[password_column]=password                                        # set hash to data series
        series[salt_column]=salt                                                # set salt to data series
    return series


  def _map_missing_user_status(self,data_series,categoty_column,hpc_user_column,hpc_user,non_hpc_user):
    '''
    An internal function for assigning user status
    '''
    if not isinstance(data_series, pd.Series):
      data_series=pd.DataFrame(data_series)

    if categoty_column not in data_series.index and not data_series[categoty_column]:
      if hpc_user_column in data_series.index and data_series[hpc_user_column]:
        data_series[categoty_column]=hpc_user                                   # assign hpc user
      else:
        data_series[categoty_column]=non_hpc_user                               # non hpc user
      
    return data_series


  def _preprocess_data(self,data, password_column='password', categoty_column='category',  \
                       email_column='email_id', hpc_user_column='hpc_username', hpc_user='HPC_USER',non_hpc_user='NON_HPC_USER', \
                       user_igf_id_column='user_igf_id', username_column='username', salt_column='encryption_salt'):
    '''
    An internal function for preprocess data before loading
    '''
    if not isinstance(data, pd.DataFrame):
      data=pd.DataFrame(data)
      
    try:
      # encrypt password
      new_data=data.apply(lambda x: self._encrypt_password(series=x),1) 

      # check email id, it should contail '@'
      new_data[email_column].map(lambda x: self._email_check(email=x))

      new_data=new_data.fillna('')
      # assign categoty, if user has hpc_username, then its 'HPC_USER'
      if categoty_column not in new_data.columns:         
        new_data[categoty_column]=None                 # add category column if it doesn't exists
      new_data.apply(lambda x: self._map_missing_user_status(data_series=x, categoty_column=categoty_column, \
                                                             hpc_user_column=hpc_user_column, hpc_user=hpc_user, \
                                                             non_hpc_user=non_hpc_user ), \
                                                             axis=1) 

      # check for username, user with igf id should have the username
      #if new_data[new_data[user_igf_id_column].notnull() & new_data[username_column].isnull()][email_column].count() > 0 :
      #  raise ValueError('Missing username for a registered user {0}'.format(new_data[user_igf_id_column].astype('str')))

      return new_data
    except:
      raise     


  def store_user_data(self, data, autosave=True):
    '''
    Load data to user table
    '''
    if not isinstance(data, pd.DataFrame):
      data=pd.DataFrame(data)     

    try:
      data=self._preprocess_data(data=data)
      self.store_records(table=User, data=data, mode='serial' )
      if autosave:
        self.commit_session()
    except:
      if autosave:
        self.rollback_session()
      raise


  def fetch_user_records_igf_id(self, user_igf_id):
    '''
    A method for fetching data for User table
    required params:
    user_igf_id: an igf id
     output_mode  : dataframe / object
    '''
    try:
      user=self.fetch_records_by_column(table=User, column_name=User.user_igf_id, column_id=user_igf_id, output_mode='one' )
      return user 
    except:
      raise
  
  
  def check_user_records_email_id(self,email_id):
    '''
    A method for checking existing user data in db
    required columns:
    email_id: An email id
    It returns True if the file is present in db or False if its not
    '''
    try:
      user_check=False
      user_obj=self.fetch_records_by_column(table=User, \
                                            column_name=User.email_id, \
                                            column_id=email_id, \
                                            output_mode='one_or_none' )
      if user_obj is not None:
        user_check=True
      return user_check
    except:
      raise
  
  