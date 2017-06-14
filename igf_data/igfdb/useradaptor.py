import json
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.igfTables import User

class UserAdaptor(BaseAdaptor):
  '''
  An adaptor class for table User
  '''

  def store_user_data(self, data):
    '''
    Load data to user table
    '''
    try:
      self.store_records(table=User, data=data)
    except:
      raise


  def fetch_user_records_igf_id(self, user_igf_id, output_mode='dataframe'):
    '''
    A method for fetching data for User table
    required params:
    user_igf_id: an igf id
     output_mode  : dataframe / object
    '''
    try:
      users=self.fetch_records_by_column(table=User, column_name=User.igf_id, column_id=user_igf_id, output_mode=output_mode )
      return users  
    except:
      raise

