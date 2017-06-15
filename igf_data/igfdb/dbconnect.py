from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

class DBConnect:
  '''
  A class for managing dbconnection
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
    data.setdefault('engine_config', {})

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
    self.dburl         = self._prepare_db_url()        # get dburl for connection
    self.engine        = self._create_session_engine() # get engine connection
    self.session_class = self._configure_session()     # get session class


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
      raise ValueError('driver {0} require dbuser, dbpass and dbhost details, {1},{2},{3}'.format(driver,dbuser,dbpass,dbhost)) # check for required parameters

    dburl='{0}'.format(driver)
    if connector:  
      dburl='{0}+{1}'.format(dburl, connector)
    dburl='{0}://'.format(dburl)
    if dbuser and dbpass and dbhost:
      dburl='{0}{1}:{2}@{3}'.format(dburl, dbuser, dbpass, dbhost)
    if dbport: 
      dburl='{0}:{1}'.format(dburl, dbport) 
    dburl='{0}/{1}'.format(dburl, dbname)
    return dburl


  def _create_session_engine(self):
    '''
    An internal method for creating an database engine required for the session 
    '''
    if not hasattr(self, 'dburl'):
      raise AttributeError('Attribute dburl not defined')
  
    try:
      engine = create_engine(self.dburl, **self.engine_config )    # create engine with additional parameter
      return engine
    except:
      raise


  def _configure_session(self):
    '''
    An internal method for preparing and configuring session
    '''
    if not hasattr(self, 'engine'):
      raise AttributeError('Attribute engine not defined')

    try:
      session_class=sessionmaker(bind=self.engine)                 # create session class
      return session_class
    except:
      raise


  def get_session_class(self):
    '''
    A method for fetching a session class 
    '''
    if not hasattr(self, 'session_class'):
      raise AttributeError('Attribute session_class not defined')

    session_class=self.session_class 
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
    except:
      raise 


  def commit_session(self):
    '''
    A method for saving changes to the database for each transection
    '''
    if not hasattr(self, 'session'):
      raise AttributeError('Attribute session not defined')

    try: 
      self.session.commit()                                        # commit session
    except:
      raise


  def close_session(self, save_changes=False):
    '''
    A method for closing a session
    It can take an optional parameter for saving 
    changes to database before closing the session
    '''
    if not hasattr(self, 'session'):
      raise AttributeError('Attribute session not defined')
 
    session=self.session
    try:
      if save_changes:
        session.commit()                                             # commit session
      session.close()                                                # close session
      self.session=None                                              # set the session attribute to None
      del self.session                                               # delete session attribute
    except:
      raise
    if hasattr(self, 'session'):
      raise AttributeError('Attribute session not deleted yet')



