import json
from igf_data.igfdb.igfTables import Base
from sqlalchemy import create_engine
from igf_data.igfdb.baseadaptor import BaseAdaptor


def read_dbconf_json(dbconfig):
  '''
  A method for reading dbconfig json file
  required params:
  dbconfig: A json file containing the database connection info
  e.g. {"dbhost":"DBHOST","dbport": PORT,"dbuser":"USER","dbpass":"DBPASS","dbname":"DBNAME","driver":"mysql","connector":"pymysql"}

  returns: a dictionary containing dbparms
  '''
  dbparam=dict()
  with open(dbconfig, 'r') as json_data:
    dbparam=json.load(json_data)

  if len(dbparam.keys())==0:
    raise ValueError('No dbconfig paramaters found in file {0}'.format(dbconfig))
  return dbparam


def clean_and_rebuild_database(dbconfig):
  '''
  A method for deleting data in database and create empty tables
  required params:
  dbconfig: A json file containing the database connection info
  '''
  try:
    dbparam=read_dbconf_json(dbconfig)
    base=BaseAdaptor(**dbparam)
    base.start_session()
    Base.metadata.drop_all(base.engine)
    Base.metadata.create_all(base.engine)
    base.close_session()
  except:
    raise
 
