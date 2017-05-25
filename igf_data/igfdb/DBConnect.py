import re
import pymysql.cursors
from abc import ABCMeta, abstractmethod

class DBConnect:
  def __init__(self, param ):
    '''
    Constructor for the Database Adaptor class
    '''
    param.setdefault('port', 3306)
    param.setdefault('driver', 'mysql')
    param.setdefault('host', '127.0.0.1')
    param.setdefault('transaction', 1)
    
    self.host=param['host']
    self.user=param['user']
    self.password=param['password']
    self.db=param['db']
    self.port=param['port']
    self.driver=param['driver']
    self.transaction=param['transaction']
 
    self.connected=0
    self.supported_drivers=self._supported_drivers_list()
    self.connection=''

  def connect(self):
    '''
    Connect to the database and return a cursor
    '''
    driver_check=0
    for driver_name,driver_pattern in self.supported_drivers.items():
      if ( re.search(driver_pattern, self.driver ) and driver_name == 'mysql'):
        '''
        Check both driver name pattern and driver name string set by the module
        '''
        driver_check += 1
        self.connected=1
        self.connection = pymysql.connect(host=self.host, user=self.user, db=self.db, port=self.port, password=self.password)
    if driver_check == 0:
      raise ValueError('DB driver {0} not supported'.format(self.driver))  

  @abstractmethod
  def prepare(self):
    '''
    Prepare sql method, implement in the child class
    '''
    pass


  def execute(self, sql_statement, input_value_list):
    '''
     Execute the statement for and return a cursor
    '''
 
    if self.connected != 1:
      raise ValueError('Can not execute, not connected to database {0} yet'.format(self.db))
    else:
      connection=self.connection
      if connection == '':
         raise ValueError('Failed to execute sql query. Connection is not available')

      for driver_name,driver_pattern in self.supported_drivers.items():
        if ( re.search(driver_pattern, self.driver ) and driver_name == 'mysql'):
          '''
          Run MySQL specific execute statement
          '''
          with connection.cursor() as cursor:
            input_value= tuple(input_value_list)
            if len(input_value) >= 1  and input_value[0] != '':
              cursor.execute( sql_statement, input_value )
            else:
              cursor.execute( sql_statement, )
            return cursor
           
  def commit(self):
    '''
    Commit changes if transaction is on
    '''
    if self.transaction > 0:
      connection=self.connection
      for driver_name,driver_pattern in self.supported_drivers.items():
        if ( re.search(driver_pattern, self.driver ) and driver_name == 'mysql'):
          '''
          Run MySQL specific execute statement
          '''
          connection.commit()
    else: 
      '''
      Do nothing if transaction is not enabled
      '''
      pass

  def disconnect(self):
    '''
    Disconnect database connection
    '''
 
    if self.connected != 1:
      raise ValueError('Can not disconnect, not connected to database {0} yet'.format(self.db))
    else:
       connection=self.connection

       for driver_name,driver_pattern in self.supported_drivers.items():
         if ( re.search(driver_pattern, self.driver ) and driver_name == 'mysql'):   
           '''
           Run MySQL specific disconnect statement
           '''
           connection.close()

           # Reset connections for object
           self.connected=0
           self.connection=''
       if self.connected == 1:
         '''
         Check connection after disconnect
         '''
         raise IOError('Failed to disconnet the database connection')

      
  def _supported_drivers_list(self):
    '''
    An internal function returns the dict of supported drivers
    '''
    driver_lists={'mysql':"MYSQL"}
    driver_paterns={driver_name:re.compile('^{0}$'.format(driver_pattern),re.IGNORECASE) for driver_name, driver_pattern in driver_lists.items()}
    return driver_paterns
 
