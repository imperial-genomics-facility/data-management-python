import pandas as pd
import json, hashlib, os, codecs
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


  def _encrypt_password(self, series, password_column='password', salt_column='encryption_salt'):
    '''
    An internal function for encrypting password
    '''
    if not isinstance(series, pd.Series):
      raise TypeError('Expecting a pandas series, got:{0}'.format(type(series)))

    series.fillna('',inplace=True)
    if not password_column in series.index or not series[password_column]:
      raise ValueError('Missing required field password: {0}'.format(json.dumps(series.to_dict())))
 
    salt=codecs.encode(os.urandom(32),"hex").decode("utf-8")                                        # calculate salt value
    password=series[password_column]                                                                # fetch password
    if not isinstance(password, str):
      password=str(series.password_column).encode('utf-8')                                          # encode password if its not a string

    if not len(password)==128:                                                                      # horrible hack for checking already hashed passward
      key=salt+password                                                                             # construct key using salt and password
      password=hashlib.sha512(str(key).encode('utf-8')).hexdigest()                                 # create password hash
      series[password_column]=password                                                              # set hash to data series
      series[salt_column]=salt                                                                      # set salt to data series
    return series


  def _preprocess_data(self,data, password_column='password', categoty_column='category', \
                       email_column='email_id', hpc_user_column='hpc_username', hpc_user='HPC_USER',
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

      # assign categoty, if user has hpc_username, then its 'HPC_USER'
      new_data[categoty_column]=new_data[hpc_user_column].notnull().map(lambda x: hpc_user if x else None)

      # check for username, user with igf id should have the username
      if new_data[new_data[user_igf_id_column].notnull() & new_data[username_column].isnull()][email_column].count() > 0 :
        raise ValueError('Missing username for a registered user {0}'.format(new_data[user_igf_id_column].astype('str')))

      new_data=new_data.fillna('')
      return new_data
    except:
      raise     


  def store_user_data(self, data):
    '''
    Load data to user table
    '''
    if not isinstance(data, pd.DataFrame):
      data=pd.DataFrame(data)     

    try:
      data=self._preprocess_data(data=data)
      self.store_records(table=User, data=data, mode='serial' )
      self.commit_session()
    except:
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


