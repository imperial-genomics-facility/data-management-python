import json
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

  def store_user_data(self, data):
    '''
    Load data to user table
    '''
    try:
      self.store_records(table=User, data=data)
    except:
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

