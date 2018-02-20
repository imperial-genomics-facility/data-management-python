from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.utils.dbutils import read_dbconf_json, read_json_data

def load_new_seqrun_data(data_file, dbconfig):
  '''
  A method for loading new data for seqrun table
  '''
  try:
    formatted_data=read_json_data(data_file)
    dbparam=read_dbconf_json(dbconfig)
    sr=SeqrunAdaptor(**dbparam)
    sr.start_session()
    sr.store_seqrun_and_attribute_data(data=formatted_data)
    sr.close_session()
  except:
    raise
