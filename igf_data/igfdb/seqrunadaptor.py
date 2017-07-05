import json
import pandas as pd
from sqlalchemy.sql import table, column
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.igfTables import Seqrun, Run, Platform

class SeqrunAdaptor(BaseAdaptor):
  '''
  An adaptor class for table Seqrun
  '''
  def store_seqrun_data(self, data):
    '''
    Load data to Seqrun table
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data=pd.DataFrame(data)

      platform_map_function=lambda x: self.map_foreign_table_and_store_attribute( \
                                              data=x, \
                                              lookup_table=Platform, \
                                              lookup_column_name='platform_igf_id', \
                                              target_column_name='platform_id')       # prepare the function for Platform id
      new_data=data.apply(platform_map_function, axis=1)                              # map platform id foreign key id
      self.store_records(table=Seqrun, data=new_data)
      self.commit_session()
    except:
      self.rollback_session()
      raise


  def fetch_seqrun_records_igf_id(self, seqrun_igf_id, target_column_name='seqrun_igf_id'):
    '''
    A method for fetching data for Seqrun table
    required params:
    seqrun_igf_id: an igf id
    target_column_name: a column name in the Seqrun table, default seqrun_igf_id
    '''
    try:
      column=[column for column in Seqrun.__table__.columns \
                       if column.key == target_column_name][0]
      seqrun=self.fetch_records_by_column(table=Seqrun, \
      	                                   column_name=column, \
      	                                   column_id=seqrun_igf_id, \
      	                                   output_mode='one')
      return seqrun  
    except:
      raise

