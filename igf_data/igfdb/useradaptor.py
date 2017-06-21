import json, hashlib
import pandas as pd
import numpy as np
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.igfTables import User

class UserAdaptor(BaseAdaptor):
  '''
  An adaptor class for table User
  '''

  def get_user_columns(self):
    '''
    A method for fetching the columns for table user
    '''
    user_column=[column.key for column in User.__table__.columns]
    return user_column


  def _email_check(self, email):
    '''
    An internal function to check if email_id has '@' or not
    require param:
    email: a string containing the email id
    '''
    if '@' not in email:
      raise ValueError('Email id {0} is not correctly formatted'.format(email))


  def _preprocess_data(self,data, password_column='password', categoty_column='category', \
                       email_column='email_id', hpc_user_column='hpc_username', hpc_user='HPC_USER',
                       user_igf_id_column='user_igf_id', username_column='username'):
    '''
    An internal function for preprocess data before loading
    '''
    if not isinstance(data, pd.DataFrame):
      data=pd.DataFrame(data)
      
    try:
      # encrypt password
      data[password_column]=data[password_column].map(lambda x: x if len(x)==128 else hashlib.sha512(str(x).encode('utf-8')).hexdigest())
      
      # check email id, it should contail '@'
      data[email_column].map(lambda x: self._email_check(email=x))

      # assign categoty, if user has hpc_username, then its 'HPC_USER'
      data[categoty_column]=data[hpc_user_column].notnull().map(lambda x: hpc_user if x else None)

      # check for username, user with igf id should have the username
      if data[data[user_igf_id_column].notnull() & data[username_column].isnull()][email_column].count() > 0 :
        raise ValueError('Missing username for a registered user {0}',format(data[user_igf_id_column].astype('str')))

      data=data.fillna('')
      return data
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

