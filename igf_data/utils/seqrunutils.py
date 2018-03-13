import datetime
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

def get_seqrun_date_from_igf_id(seqrun_igf_id):
  '''
  A utility method for fetching sequence run date from the igf id
  
  required params:
  seqrun_igf_id: A seqrun igf id string
  
  returns a string value of the date
  '''
  try:
    seqrun_date=seqrun_igf_id.split('_')[0]                                     # collect partial seqrun date from igf id
    seqrun_date=datetime.datetime.strptime(seqrun_date,'%y%m%d').date()         # identify actual date
    seqrun_date=str(seqrun_date)                                                # convert object to string
    return seqrun_date
  except:
    raise