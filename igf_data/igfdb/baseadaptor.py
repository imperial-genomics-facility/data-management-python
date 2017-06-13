import json
import warnings
import pandas as pd
from flask.ext.sqlalchemy import exc
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
 
exceptions = exc.sa_exc

class BaseAdaptor:
  '''
  The base adaptor class
  '''
  
  def __init__(self, **data):
    data.setdefault('dbhost', '')
    data.setdefault('dbport', '')
    data.setdefault('dbuser', '')
    data.setdefault('dbpass', '')
    data.setdefault('dbname', '')
    data.setdefault('driver', 'sqlite')
    data.setdefault('connector', '')
    data.setdefault('supported_drivers', ('mysql', 'sqlite'))
    data.setdefault('engine_config', '')
    
   
    self.dbhost    = data['dbhost']
    self.dbport    = data['dbport']
    self.dbuser    = data['dbuser']
    self.dbpass    = data['dbpass']
    self.dbname    = data['dbname']
    self.driver    = data['driver']
    self.connector = data['connector']
    self.supported_drivers = data['supported_drivers']
    self.engine_config     = data['engine_config']
  
    # create engine and configure session at start up
    self._prepare_db_url()
    self._create_session_engine()
    self._configure_session()
    
  def _prepare_db_url(self):
    '''
    An internal method for preparing url for database connection
    '''
    dbhost    = self.dbhost
    dbport    = self.dbport
    dbuser    = self.dbuser
    dbpass    = self.dbpass
    dbname    = self.dbname
    driver    = self.driver
    connector = self.connector

    if driver not in self.supported_drivers:
      raise ValueError('Database driver {0} is not supported yet.'.format(driver)) # check for supported databases

    if driver != 'sqlite' and (not dbuser or not dbpass or not dbhost):
      raise ValueError('driver {0} require dbuser, dbpass and dbhost details'.format(driver)) # check for required parameters

    dburl='{0}'.format(driver)

    if connector:  
      dburl='{0}+{1}'.format(dburl, connector)

    dburl='{0}://'.format(dburl)
 
    if dbuser and dbpass and dbhost:
      dburl='{0}{1}:{2}@{3}'.format(dburl, dbuser, dbpass, dbhost)

    if dbport: 
      dburl='{0}:{1}'.format(dburl, dbport) 

    dburl='{0}/{1}'.format(dburl, dbname)

    self.dburl=dburl


  def _create_session_engine(self):
    '''
    An internal method for creating an database engine required for the session 
    '''
    
    if not hasattr(self, dburl):
      raise AttributeError('Attribute dburl not found')           # raise exception if attribute dburl is not present
  
    try:
      self.engine = create_engine(self.dburl, self.engine_config) # create engine
    except:
      raise

  def _configure_session(self):
    '''
    An internal method for preparing and configuring session
    '''
    
    if not hasattr(self, engine):
      raise AttributeError('Attribute engine not found')           # raise exception if engine attribute is not assigned
 
    try:
      self.session_class=sessionmaker(bind=self.engine)            # create session class
    except:
      raise

  def start_session(self):
    '''
    A method for creating a new session
    '''
    if not hasattr(self, session_class):
      raise AttributeError('Attribute session_class not found')     # raise exception if session_class attribute is not present
 
    if hasattr(self, session):
      raise AttributeError('Attribute session is already present')  # raise exception if session attribute is already present

    Session=self.session_class

    try:
      self.session=Session()                                        # create session
    except:
      raise

  def close_session(self, save_changes=False):
    '''
    A method for closing a session
    It can take an optional parameter for saving 
    changes to database before closing the session
    '''

    if not hasattr(self, session):
      raise AttributeError('Attribute session not found')
 
    session=self.session

    try:
      if save_changes:
        session.commit()                                             # commit session
      session.close()                                                # close session
      self.session=None                                              # set the session attribute to None
      del self.session                                               # delete session attribute
    except:
      raise

    
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
