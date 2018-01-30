from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.utils.dbutils import read_dbconf_json, read_json_data

def load_new_platform_data(data_file, dbconfig):
  '''
  A method for loading new data for platform table
  '''
  try:
    formatted_data=read_json_data(data_file)
    dbparam=read_dbconf_json(dbconfig)
    pl=PlatformAdaptor(**dbparam)
    pl.start_session()
    pl.store_platform_data(data=formatted_data)
    pl.close_session()
  except:
    raise

def load_new_flowcell_data(data_file, dbconfig):
  '''
  A method for loading new data to flowcell table
  '''
  try:
    flowcell_rule_data=read_json_data(data_file)
    dbparam=read_dbconf_json(dbconfig)
    pl=PlatformAdaptor(**dbparam)
    pl.start_session()
    pl.store_flowcell_barcode_rule(data=flowcell_rule_data)
    pl.close_session()
  except:
    raise