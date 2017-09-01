from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.utils.dbutils import read_dbconf_json, read_json_data


def load_new_pipeline_data(data_file, dbconfig):
  '''
  A method for loading new data for pipeline table
  '''
  try:
    formatted_data=read_json_data(data_file)
    dbparam=read_dbconf_json(dbconfig)
    pp=PipelineAdaptor(**dbparam)
    pp.start_session()
    pp.store_pipeline_data(data=formatted_data)
    pp.close_session()
  except:
    raise
