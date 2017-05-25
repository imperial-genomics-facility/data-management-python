import re

class DBAdaptor:
  def __init__(self, host, user, password, db, port=3306, driver='mysql' ):
    '''
    Constructor for the Database Adaptor class
    '''
    self.host=host
    self.user=user
    self.password=password
    self.db=db
    self.port=port
    self.driver=driver
    self.connected=0
    self.supported_drivers=self._supported_drivers_list()

  def connect(self):
    '''
    Connect to the database and return a cursor
    '''
    driver_check=0
    for driver_name,driver_pattern in self.supported_drivers.items():
      if ( re.search(driver_pattern, self.driver )):
        driver_check += 1
        print('Connecting to driver {0}'.format(self.driver))
        self.connected=1
    if driver_check == 0:
      raise ValueError('DB driver {0} not supported'.format(self.driver))  

  def prepare(self):
    '''
    Prepare sql method, implement in the child class
    '''
    raise ValueError('No Prepare method is definded in the child class')

  def execute(self, sql_statement, input_values):
    '''
     Execute the statement for and return a cursor
    '''
 
    if self.connected != 1:
      raise ValueError('Can not execute, not connected to database {0} yet'.format(self.db))
      
     
  
  def disconnect(self):
    '''
    Disconnect database connection
    '''
 
    if self.connected != 1:
      raise ValueError('Can not disconnect, not connected to database {0} yet'.format(self.db))
    else:
       print('Disconnecting from database {0}'.format(self.db))

  def _supported_drivers_list(self):
    '''
    An internal function returns the dict of supported drivers
    '''
    driver_lists=["MYSQL"]
    driver_paterns=dict(driver:re.compile('^{0}$'.format(driver),re.IGNORECASE) for driver in driver_lists)
    return driver_paterns
 

if __name__=='__main__':
  tdb=DBAdaptor(host='host1', user='user1', password='pass', db='igfdb', driver='MySQL')
  tdb.connect()
  tdb.disconnect()
