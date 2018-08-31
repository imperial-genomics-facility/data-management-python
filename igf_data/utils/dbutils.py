import os,json
from igf_data.igfdb.igfTables import Base
from sqlalchemy import create_engine
from igf_data.igfdb.baseadaptor import BaseAdaptor

def read_json_data(data_file):
  '''
  A method for reading data from json file

  :param data_file: A Json format file
  :returns: A list of dictionaries
  '''
  data=None
  try:
    if not os.path.exists(data_file):
      raise IOError('file {0} not found'.format(data_file))
    
    with open(data_file, 'r') as json_data: 
      data=json.load(json_data)
  
    if data is None:
      raise ValueError('No data found in file {0}'.format(data))

    if not isinstance(data, list):
      data=[data]                                                               # convert data dictionary to a list of dictionaries
    return data
  except:
    raise


def read_dbconf_json(dbconfig):
  '''
  A method for reading dbconfig json file
  
  :param dbconfig: A json file containing the database connection info
                   e.g. {"dbhost":"DBHOST","dbport": PORT,"dbuser":"USER","dbpass":"DBPASS","dbname":"DBNAME","driver":"mysql","connector":"pymysql"}

  :returns: a dictionary containing dbparms
  '''
  dbparam=dict()
  try:
    with open(dbconfig, 'r') as json_data:
      dbparam=json.load(json_data)

    if len(dbparam.keys())==0:
      raise ValueError('No dbconfig paramaters found in file {0}'.format(dbconfig))
    return dbparam
  except:
    raise


def clean_and_rebuild_database(dbconfig):
  '''
  A method for deleting data in database and create empty tables
  
  :param  dbconfig: A json file containing the database connection info
  '''
  try:
    dbparam=read_dbconf_json(dbconfig)
    base=BaseAdaptor(**dbparam)
    Base.metadata.drop_all(base.engine)
    Base.metadata.create_all(base.engine)
  except:
    raise
 
