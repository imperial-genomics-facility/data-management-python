import json
import pandas as pd
from sqlalchemy.sql import table, column
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.igfTables import Seqrun, Run

class SeqrunAdaptor(BaseAdaptor):
  '''
  An adaptor class for table Seqrun
  '''
  def store_seqrun_data(self, data):
    '''
    Load data to Seqrun table
    '''
    try:
      self.store_records(table=Seqrun, data=data)
    except:
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

