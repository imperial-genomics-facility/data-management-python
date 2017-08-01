import json
import pandas as pd
from sqlalchemy.sql import table, column
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.igfTables import Platform, Flowcell_barcode_rule

class PlatformAdaptor(BaseAdaptor):
  '''
  An adaptor class for Platform tables
  '''

  def store_platform_data(self, data, autosave=True):
    '''
    Load data to Platform table
    '''
    try:
      self.store_records(table=Platform, data=data)
      if autosave:
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

  def store_flowcell_barcode_rule(self, data, autosave=True):
    '''
    Load data to flowcell_barcode_rule table
    required params:
    data: A dictionary or dataframe containing following columns
          platform_igf_id / platform_id
          flowcell_type
          index_1 (NO_CHANGE/REVCOMP/UNKNOWN)
          index_2 (NO_CHANGE/REVCOMP/UNKNOWN)
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data=pd.DataFrame(data)
 
      if 'platform_igf_id' in data.columns:
        platform_map_function=lambda x: self.map_foreign_table_and_store_attribute( \
                                                 data=x, \
                                                 lookup_table=Platform, \
                                                 lookup_column_name='platform_igf_id', \
                                                 target_column_name='platform_id')    # prepare the function for Platform id
        new_data=data.apply(platform_map_function, axis=1)                                # map platform id foreign key id
        data=new_data

      self.store_records(table=Flowcell_barcode_rule, data=data)
      if autosave:
        self.commit_session()
    except:
      if autosave:
        self.rollback_session()
      raise


