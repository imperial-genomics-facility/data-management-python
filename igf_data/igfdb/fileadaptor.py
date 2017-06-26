import json
import pandas as pd
from sqlalchemy.sql import table, column
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.igfTables import File 

class FileAdaptor(BaseAdaptor):
  '''
  An adaptor class for File tables
  '''
  def get_file_columns(self):
    '''
    A method for fetching the columns for table file
    '''
    file_column=[column.key for column in File.__table__.columns]
    return user_column

  
  def store_file_data(self, data):
    '''
    Load data to file table
    '''
    if not isinstance(data, pd.DataFrame):
      data=pd.DataFrame(data)     

    try:
      self.store_records(table=File, data=data, mode='bulk' )
      self.commit_session()
    except:
      self.rollback_session()
      raise


  def fetch_file_records_file_path(self, file_path):
    '''
    A method for fetching data for file table
    required params:
    file_path: an absolute file path
    '''
    try:
      file_obj=self.fetch_records_by_column(table=File, column_name=File.file_path, column_id=file_path, output_mode='one')
      return file_obj
    except:
      raise
