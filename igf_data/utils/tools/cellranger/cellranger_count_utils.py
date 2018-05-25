import os
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
    fastq_dir_list=[]                                                           # default list of fastq dirs
    dbconnected=False
    base=BaseAdaptor(**{'session_class':db_session_class})
    base.start_session()
    dbconnected=True
    subquery=base.session.\
             query(Run.run_igf_id).\
             join(Experiment).\
             filter(Run.experiment_id==Experiment.experiment_id).\
             filter(Experiment.experiment_igf_id==experiment_igf_id)            # get subquery for run_igf_ids
    query=base.session.\
          query(File.file_path).\
          join(Collection).\
          join(Collection_group).\
          filter(Collection.collection_id==Collection_group.collection_id).\
          filter(Collection_group.file_id==File.file_id).\
          filter(Collection.type==fastq_collection_type).\
          filter(Collection.name.in_(subquery))                                 # select file_path linked to run_igf_ids
    results=base.fetch_records(query=query,
                               output_mode='object')                            # fetch results as generator expression
    for row in results:
      fastq_dir_list.append(os.path.dirname(row.file_path))                     # add fastq directory path to the putput list
    if len(fastq_dir_list) > 0:
      fastq_dir_list=list(set(fastq_dir_list))                                  # remove redundant values
    base.close_session()
    return fastq_dir_list                                                       # return list of fastq dirs
  except:
    if dbconnected:
      base.close_session()
    raise