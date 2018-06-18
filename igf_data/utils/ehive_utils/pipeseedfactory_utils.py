import datetime
import pandas as pd
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor

def get_pipeline_seeds(pipeseed_mode,pipeline_name,igf_session_class,
                       seed_id_label='seed_id',seqrun_date_label='seqrun_date',
                       seqrun_id_label='seqrun_id',experiment_id_label='experiment_id',
                       seqrun_igf_id_label='seqrun_igf_id'):
  '''
  A utils function for fetching pipeline seed information
  
  :param pipeseed_mode: A string info about pipeseed mode, allowed values are
                          demultiplexing
                          alignment
  
  :param pipeline_name: A string infor about pipeline name
  :param igf_session_class: A database session class for pipeline seed lookup
  :returns: Two Pandas dataframes, first with pipeseed entries and second with seed info
  '''
  try:
    if pipeseed_mode not in ('demultiplexing','alignment'):
      raise ValueError('Pipeseed_mode {0} not supported'.format(pipeseed_mode))

    table_name=None
    if pipeseed_mode=='demultiplexing':
      table_name='seqrun'
    elif pipeseed_mode=='alignment':
      table_name='experiment'

    pa = PipelineAdaptor(**{'session_class':igf_session_class})                 # get db adaptor
    pa.start_session()                                                          # connect to db
    dbconnected=True
    pipeseeds_data, table_data = \
            pa.fetch_pipeline_seed_with_table_data(pipeline_name,
                                                   table_name=table_name)       # fetch requires entries as list of dictionaries from table for the seeded entries
    seed_data=pd.DataFrame()
    if not isinstance(pipeseeds_data,pd.DataFrame) or \
       not isinstance(table_data,pd.DataFrame):
      raise AttributeError('Expecting a pandas dataframe of pipeseed data and received {0}, {1}').\
                           format(type(pipeseeds_data),type(table_data))

    if len(pipeseeds_data.index) > 0 and \
       len(table_data.index) > 0:
      pipeseeds_data[seed_id_label]=pipeseeds_data[seed_id_label].\
                                    map(lambda x: int(x))                       # convert pipeseed column type
      if pipeseed_mode=='demultiplexing':
        table_data[seqrun_id_label]=table_data[seqrun_id_label].\
                                    map(lambda x: int(x))                       # convert seqrun data column type
        merged_data=pd.merge(pipeseeds_data,
                             table_data,
                             how='inner',
                             on=None,
                             left_on=[seed_id_label],
                             right_on=[seqrun_id_label],
                             left_index=False,
                             right_index=False)                                 # join dataframes
        merged_data[seqrun_date_label]=\
             merged_data[seqrun_igf_id_label].\
             map(lambda x:  _get_date_from_seqrun(seqrun_igf_id=x))             # get seqrun date from seqrun id
      elif pipeseed_mode=='alignment':
        table_data[experiment_id_label]=table_data[experiment_id_label].\
                                        map(lambda x: int(x))                   # convert experiment data column type
        merged_data=pd.merge(pipeseeds_data,
                             table_data,
                             how='inner',
                             on=None,
                             left_on=[seed_id_label],
                             right_on=[experiment_id_label],
                             left_index=False,
                             right_index=False)                                 # join dataframes

      seed_data=merged_data.\
                applymap(lambda x: str(x))                                      # convert dataframe to string and add as list of dictionaries
    return pipeseeds_data, seed_data
  except:
    raise


def _get_date_from_seqrun(seqrun_igf_id):
  '''
  An internal method for extracting sequencing run date from seqrun_igf_id

  :param seqrun_igf_id: A sequencing run id , eg 171001_XXX_XXX-XXX
  :returns: A datetime object with the seqrun date
  '''
  try:
    seqrun_date=seqrun_igf_id.split('_')[0]                                     # set first part of the string, e.g., 171001_XXX_XXX-XXX
    seqrun_date=datetime.datetime.strptime(seqrun_date,'%y%m%d').date()         # convert it to date object
    return seqrun_date
  except:
    raise
