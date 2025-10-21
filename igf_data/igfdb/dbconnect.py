from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

class DBConnect:
  '''
  A class for managing dbconnection
  '''
  def __init__(self, **data):
    self.dbhost            = data.get('dbhost', '')
    self.dbport            = data.get('dbport', '')
    self.dbuser            = data.get('dbuser', '')
    self.dbpass            = data.get('dbpass', '')
    self.dbname            = data.get('dbname', '')
    self.driver            = data.get('driver', 'sqlite')
    self.connector         = data.get('connector', '')
    self.supported_drivers = data.get('supported_drivers', ('mysql', 'sqlite'))
    self.engine_config     = data.get('engine_config', {})
    self.connect_args      = data.get('connect_args', {})
    # create engine and configure session at start up
    data_url = data.get('url', '')
    if data_url == '':
      self.dburl = self._prepare_db_url()                                       # get dburl for connection
    else:
      self.dburl = data_url
    self.engine        = self._create_session_engine()                          # get engine connection
    self.session_class = self._configure_session()                              # get session class


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
      raise ValueError(
        f'Database driver {driver} is not supported yet.') # check for supported databases

    if driver != 'sqlite' and (not dbuser or not dbhost):
      raise ValueError(
        f'Driver {driver} require dbuser and dbhost details, {dbuser},{dbhost}') # check for required parameters

    dburl = driver
    if connector:  
      dburl = f'{dburl}+{connector}'
    dburl = f'{dburl}://'
    if dbuser and dbpass and dbhost:
      dburl = f'{dburl}{dbuser}:{dbpass}@{dbhost}'
    if dbport: 
      dburl = f'{dburl}:{dbport}'
    dburl = f'{dburl}/{dbname}'
    return dburl


  def _create_session_engine(self):
    '''
    An internal method for creating an database engine required for the session 
    '''
    if not hasattr(self, 'dburl'):
      raise AttributeError(
        'Attribute dburl not defined')
    try:
      engine = \
        create_engine(
          self.dburl,
          connect_args=self.connect_args,
          **self.engine_config )    # create engine with additional parameter
      return engine
    except Exception:
      raise


  def _configure_session(self):
    '''
    An internal method for preparing and configuring session
    '''
    if not hasattr(self, 'engine'):
      raise AttributeError('Attribute engine not defined')

    try:
      session_class = \
        sessionmaker(bind=self.engine) # create session class
      return session_class
    except Exception:
      raise


  def get_session_class(self):
    '''
    A method for fetching a session class 
    '''
    if not hasattr(self, 'session_class'):
      raise AttributeError('Attribute session_class not defined')

    session_class = self.session_class 
    return session_class


  def start_session(self):
    '''
    A method for creating a new session
    '''
    if not hasattr(self, 'session_class'):
      raise AttributeError('Attribute session_class not defined')
    
    if hasattr(self, 'session'):
      raise AttributeError('Attribute session already defined')

    Session=self.session_class
    try:
      self.session=Session()                                       # create session
    except Exception:
      raise 


  def commit_session(self):
    '''
    A method for saving changes to the database for each transection
    '''
    if not hasattr(self, 'session'):
      raise AttributeError('Attribute session not defined')

    try: 
      self.session.commit()                                        # commit session
    except Exception:
      raise

  def rollback_session(self):
    '''
    A method for rollback changes for the session
    '''
    try:
      self.session.rollback()
    except Exception:
      raise
      
  def close_session(self, save_changes=False):
    '''
    A method for closing a session
    It can take an optional parameter for saving 
    changes to database before closing the session
    '''
    if not hasattr(self, 'session'):
      raise AttributeError('Attribute session not defined')
 
    session = self.session
    try:
      if save_changes:
        session.commit()                                             # commit session
      session.close()                                                # close session
      self.session=None                                              # set the session attribute to None
      del self.session                                               # delete session attribute
    except Exception:
      raise
    if hasattr(self, 'session'):
      raise AttributeError('Attribute session not deleted yet')



