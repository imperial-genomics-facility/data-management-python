
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.experimentadaptor import ExperimentAdaptor
from igf_data.igfdb.igfTables import File,Experiment,Run,Collection,Collection_group


def get_cellranger_input_list(db_session_class,experiment_igf_id,
                              fastq_collection_type='demultiplexed_fastq'):
  '''
  A function for fetching input list for cellranger count run for a specific experiment
  
  :param db_session_class: A database session class
  :param experiment_igf_id: An experiment igf id
  :param fastq_collection_type: Fastq collection type name, default demultiplexed_fastq
  :returns: A list of fastq dir path for the cellranger count run
  '''
  try:
    dbconnected=False
    base=BaseAdaptor(**{'session_class':db_session_class})
    base.start_session()
    dbconnected=True
    
    base.close_session()
  except:
    if dbconnected:
      base.close_session()
    raise