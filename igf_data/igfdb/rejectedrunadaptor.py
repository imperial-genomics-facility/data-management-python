import json
import pandas as pd
from sqlalchemy.sql import table, column
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.igfTables import Rejected_run, Run

  def store_rejected_run_data(self, data):
    '''
    Load data to Rejected_run table
    '''
    try:
      self.store_records(table=Rejected_run, data=data)
    except:
      raise


  def fetch_rejected_run_records_igf_id(self, seqrun_igf_id, target_column_name='seqrun_igf_id'):
    '''
    A method for fetching data for Rejected_run table
    required params:
    seqrun_igf_id: an igf id
    target_column_name: a column name in the Rejected_run table, default seqrun_igf_id
    '''
    try:
      column=[column for column in Rejected_run.__table__.columns \
                       if column.key == target_column_name][0]
      rejected_run=self.fetch_records_by_column(table=Rejected_run, \
      	                                   column_name=column, \
      	                                   column_id=seqrun_igf_id, \
      	                                   output_mode='one')
      return rejected_run  
    except:
      raise
