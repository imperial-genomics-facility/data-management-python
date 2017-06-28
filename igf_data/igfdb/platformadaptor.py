import json
import pandas as pd
from sqlalchemy.sql import table, column
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.igfTables import Platform

class PlatformAdaptor(BaseAdaptor):
  '''
  An adaptor class for Platform tables
  '''

  def store_platform_data(self, data):
    '''
    Load data to Platform table
    '''
    try:
      self.store_records(table=Platform, data=data)
      self.commit_session()
    except:
      self.rollback_session()
      raise


  def fetch_platform_records_igf_id(self, platform_igf_id, target_column_name='platform_igf_id'):
    '''
    A method for fetching data for Platform table
    required params:
    platform_igf_id: an igf id
    target_column_name: column name in the Platform table, default is platform_igf_id
    '''
    try:
      column=[column for column in Platform.__table__.columns \
                       if column.key == target_column_name][0]
      platform=self.fetch_records_by_column(table=Platform, \
      	                                   column_name=column, \
      	                                   column_id=platform_igf_id, \
      	                                   output_mode='one')
      return platform  
    except:
      raise
