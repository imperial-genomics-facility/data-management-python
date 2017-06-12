import json
import warnings
import pandas as pd
from flask.ext.sqlalchemy import exc
from sqlalchemy.orm import sessionmaker
 
exceptions = exc.sa_exc

class BaseAdaptor:
  '''
  The base adaptor class
  '''
  
  def __init__(self, engine):
    self.engine  = engine

  def load_data(self, table, data_frame, mode='serial'):
    '''
    A method for loading data to table
    '''

    if not isinstance(data_frame, pd.DataFrame):
      raise ValueError('Expected pandas dataframe as input')

    if mode not in ('serial', 'bulk'):
      raise ValueError('Mode {} is not recognised'.format(mode))

    engine=self.engine
    Session=sessionmaker(bind=engine)
    session=Session()

    if mode is 'serial':
      data_frame=data_frame.fillna('')
      data_frame_dict=data_frame.to_dict()
      
      try:
        data_frame_dict={ key:value for key, value in data_frame_dict.items() if value} # filter any empty value
        mapped_object=table(**data_frame_dict)
        session.add(mapped_object)
        session.flush()
      except exceptions.SQLAlchemyError:
        session.rollback()
        warnings.warn("Couldn't load record to table {0}: {1} ".format(table,json.dumps(data_frame_dict)))
      finally:
        session.commit()

    elif mode is 'bulk':
      data_dict=data_frame.to_dict(orient='records')
     
      try:
        session.bulk_insert_mappings(table, data_dict)
        session.flush()
      except exceptions.SQLAlchemyError:
        session.rollback()
        warnings.warn("Couldn't load record to table {0}: {1} ".format(table,json.dumps(data_frame_dict)))
      finally:
        session.commit()

    session.close()    

  def load_attributes(self, attribute_table, data ):
    '''
    A method for loading data to attribute table
    '''
    self.load_data(table=attribute_table)
    pass 

  def get_table_info_by_igf_id(self, table, igf_id):
    '''
    A method for fetching record with the igf_id
    '''
    pass

  def get_attributes_by_dbid(self, attribute_table, db_id):
    '''
    A method for fetching attribute records for a specific attribute table with a db_id linked as foreign key
    '''
    pass
