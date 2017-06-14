import json
import warnings
import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
 
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
    data.setdefault('driver', 'mysql')
    data.setdefault('connector', 'pymysql')
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
      engine = create_engine(self.dburl, **self.engine_config )           # create engine with additional parameter
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


  def _store_record_serial(self, table, data):
    '''
    An internal method for storing dataframe records in serial mode
    '''
    if not hasattr(self, 'session'):
      raise AttributeError('Attribute session not found')
    
    if not isinstance(data, pd.Series):
      raise  ValueError('Expecting a Pandas dataframe and recieved data type: {0}'.format(type(data)))

    session=self.session
    data=data.fillna('')
    data_frame_dict=data.to_dict()
    try:
      data_frame_dict={ key:value for key, value in data_frame_dict.items() if value} # filter any key with empty value
      mapped_object=table(**data_frame_dict)                                          # map dictionary to table class
      session.add(mapped_object)                                                      # add data to session
    except:
      raise
    

  def _store_record_bulk(self, table, data):
    '''
    An internal method for storing dataframe records in bulk mode
    '''   
    if not hasattr(self, 'session'):
      raise AttributeError('Attribute session not found')
 
    if isinstance(data, pd.DataFrame):
      data=data.to_dict(orient='records')

    session=self.session
    if not isinstance(data, dict):
      raise ValueError('Expecting a dictionary and recieved data type: {0}'.format(type(data)))
 
    try:
      session.bulk_insert_mappings(table, data)
    except:
      raise 


  def store_records(self, table, data, mode='serial'):
    '''
    A method for loading data to table
    required parameters:
    table: name of the table class
    data : pandas dataframe or a list of dictionary
    mode : serial / bulk
    '''
    if not hasattr(self,'session'):
      raise AttributeError('Attribute session not found')

    if mode not in ('serial', 'bulk'):
      raise ValueError('Mode {0} is not recognised'.format(mode))

    if not isinstance(data, pd.DataFrame):
      data=pd.DataFrame(data)                                                        # convert dicttionary to dataframe

    session=self.session
    try:
      if mode is 'serial':
        data.apply(lambda x: self._store_record_serial(table=table, data=x), axis=1)   # load data in serial mode
      elif mode is 'bulk':
        self._store_record_bulk( table=table, data=data)                               # load data in bulk mode
      session.flush()
    except:
        session.rollback()
        raise


  def store_attributes(self, attribute_table, linked_column, db_id, data, mode='serial'):
    '''
    A method for storing attributes
    required params:
    attribute_table: a attribute table name
    linked_column  : a column name to link the db_id to attribute table
    db_id          : a db_id to link the attribute records
    mode           : serial / bulk
    '''
    if isinstance(data, dict):
      data=pd.DataFrame(data)                                                       # converting to dataframe
     
    data[linked_column]=db_id                                                       # adding db_id value for the linked column
    try:
      self.store_records(table=attribute_table, data=data, mode=mode)               # storing data to attribute table
    except:
      raise


  def modify_records(self, query, update_values):
    '''
    A method for updating table records
    required params:
    query: a session.query object with filter criteria
    update_values: a dictionaries, with key as the column 
                   name and value as the new updated value
    '''
    if not hasattr(self, 'session'):
      raise AttributeError('Attribute session not found')

    session=self.session 
    for row in query:
      try:
        for column_name, new_value in update_values.items():
          row.column_name=new_value
        session.commit()
      except:
        session.rollback()


  def _fetch_records_as_dataframe(self, query):
    '''
    An internal method for fetching database records as dataframe
    '''
    if not hasattr(self, 'session'):
      raise AttributeError('Attribute session not found')

    session=self.session
    try:
      result=pd.read_sql(query.statement, session.bind)
      return result                                      # return a dataframe
    except:
      raise
  
 
  def _fetch_records_as_object(self, query):
    '''
    An internal method for fetching database records as query object
    '''
    result=list()
    for row in query:
      yield row                                          # return a generator object


  def _construct_query(self, table, filter_criteria ):
    '''
    An internal method for query construction
    It doesn't support any join statement
    required params:
    table: table name class
    filter_criteria: a list of criteria, each filter statement has three values, column_name, operator and value
    return a session.query object
    '''   
    if not hasattr(self, 'session'):
      raise AttributeError('Attribute session not found')

    if not isinstance(filter_criteria, tuple) and  not isinstance(filter_criteria, list):
      raise ValueError('Expecting a list of filter_criteria, received data type: {0}'.format(type(filter_criteria)))

    session=self.session
    query=session.query(table)
    for filter_statement in filter_criteria:
      if len(filter_statement) != 3:
        raise ValueError('Expecting three parameters for filter criteria, got {0}'.format(len(filter_statement)))

      if filter_statement[1] == '==':
        query=query.filter(filter_statement[0] == filter_statement[2])
    return query  

    
  def fetch_records(self, query, output_mode='dataframe'):
    '''
    A method for fetching records using a query
    optional parameters:
    output_mode: dataframe / object
    
    returns a pandas dataframe for dataframe mode and a generator object for object mode
    ''' 
    if output_mode not in ('dataframe', 'object'):
      raise ValueError('Expecting output_mode as dataframe or object, no support for {0}'.format(output_mode))

    result=''
    if output_mode == 'dataframe':
      result=self._fetch_records_as_dataframe(query=query)  # result is a dataframe
    elif output_mode == 'object':
      result=self._fetch_records_as_object(query=query)     # result is a generator object
    return result

    
  def fetch_records_by_column(self, table, column_name, column_id, output_mode='dataframe'):
    '''
    A method for fetching record with the column
    required param:
    table      : table name
    column_name: a column name
    column_id  : a column id value
    output_mode: dataframe / object
    '''
    if not hasattr(self, 'session'):
      raise AttributeError('Attribute session not found')
 
    print(column_name)
    session=self.session
    query=session.query(table).filter(column_name==column_id)
    try:
      result=self.fetch_records(query=query, output_mode=output_mode)
      return result
    except:
      raise


  def get_attributes_by_dbid(self, attribute_table, linked_table, linked_column, db_id):
    '''
    A method for fetching attribute records for a specific attribute table with a db_id linked as foreign key
    '''
    session=self.session
    query=session.query(linked_table).join(linked_table)
    filter_criteria=[linked_table.linked_column==db_id]
    try:
      result=self.fetch_records(query=query, filter_criteria=filter_criteria)
      return result
    except:
      raise


