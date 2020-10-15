import pandas as pd
from sqlalchemy.sql import column
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.igfTables import Platform, Flowcell_barcode_rule

class PlatformAdaptor(BaseAdaptor):
  '''
  An adaptor class for Platform tables
  '''

  def store_platform_data(self,data,autosave=True):
    '''
    Load data to Platform table

    :param data: A list of dictionaries or a pandas dataframe
    :param autosave: A toggle for autocommit, default False
    :returns: None
    '''
    try:
      self.store_records(
        table=Platform,
        data=data)
      if autosave:
        self.commit_session()
    except Exception as e:
      self.rollback_session()
      raise ValueError(
              'Failed to store platform data, error: {0}'.format(e))


  def fetch_platform_records_igf_id(
        self,platform_igf_id,target_column_name='platform_igf_id',
        output_mode='one'):
    '''
    A method for fetching data for Platform table
    
    :param platform_igf_id: an igf id
    :param target_column_name: column name in the Platform table, default is platform_igf_id
    :returns: Platform record as object
    '''
    try:
      column = [
        column
          for column in Platform.__table__.columns \
            if column.key == target_column_name][0]
      platform = \
        self.fetch_records_by_column(
          table=Platform,
      	  column_name=column,
      	  column_id=platform_igf_id,
      	  output_mode=output_mode)
      return platform  
    except Exception as e:
      raise ValueError(
              'Failed to fetch platform data, error: {0}'.format(e))

  def store_flowcell_barcode_rule(self,data,autosave=True):
    '''
    Load data to flowcell_barcode_rule table
    
    :param data: A dictionary or dataframe containing following columns
          
                 * platform_igf_id / platform_id
                 * flowcell_type
                 * index_1 (NO_CHANGE/REVCOMP/UNKNOWN)
                 * index_2 (NO_CHANGE/REVCOMP/UNKNOWN)
    :param autosave: A toggle for autocommit, default True
    :returns: None
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data = pd.DataFrame(data)
 
      if 'platform_igf_id' in data.columns:
        platform_map_function = \
          lambda x: \
            self.map_foreign_table_and_store_attribute(
              data=x,
              lookup_table=Platform,
              lookup_column_name='platform_igf_id',
              target_column_name='platform_id')                                 # prepare the function for Platform id
        data['platform_id'] = ''
        data = \
          data.apply(
            platform_map_function,
            axis=1,
            result_type=None)                                                   # map platform id foreign key id
        data.drop(
          'platform_igf_id',
          axis=1,
          inplace=True)
        #data=new_data

      self.store_records(table=Flowcell_barcode_rule, data=data)
      if autosave:
        self.commit_session()
    except Exception as e:
      if autosave:
        self.rollback_session()
      raise ValueError(
              'Failed to store barcore rules, error: {0}'.format(e))
