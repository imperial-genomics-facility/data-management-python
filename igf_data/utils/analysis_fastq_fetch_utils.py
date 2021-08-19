import os
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.experimentadaptor import ExperimentAdaptor
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.igfTables import File,Experiment,Run,Collection,Collection_group,Sample

def get_fastq_and_run_for_samples(
      dbconfig_file,sample_igf_id_list,active_status='ACTIVE',
      combine_fastq_dir=False,fastq_collection_type='demultiplexed_fastq'):
  """
  A function for fetching fastq and run_igf_id for a list od samples

  :param dbconfig_file: DB config file path
  :param sample_igf_id_list: A list of sample_igf_ids for DB lookup
  :param active_status: Filter tag for active experiment, run and file status, default: active
  :param combine_fastq_dir: Combine fastq file directories for output line, default False
  :param fastq_collection_type: Fastq collection type, default 'demultiplexed_fastq'
  :returns: A list of dictionary containing the sample_igf_id, run_igf_id and file_paths
  """
  try:
    fastq_output_list = list()
    dbconf = read_dbconf_json(dbconfig_file)
    base = BaseAdaptor(**dbconf)
    base.start_session()
    query = \
      base.session.\
        query(Sample.sample_igf_id,Run.run_igf_id,File.file_path).\
        join(Experiment,Sample.sample_id==Experiment.sample_id).\
        join(Run,Experiment.experiment_id==Run.experiment_id).\
        join(Collection,Collection.name==Run.run_igf_id).\
        join(Collection_group,Collection.collection_id==Collection_group.collection_id).\
        join(File,File.file_id==Collection_group.file_id).\
        filter(Run.status==active_status).\
        filter(Experiment.status==active_status).\
        filter(File.status==active_status).\
        filter(Collection.type==fastq_collection_type).\
        filter(Sample.sample_igf_id.in_(sample_igf_id_list))
    results = \
      base.fetch_records(
        query=query,
        output_mode='dataframe')
    base.close_session()
    if combine_fastq_dir:
      results['file_path'] = \
        results['file_path'].\
          map(lambda x: os.path.dirname(x))
      results.drop_duplicates(inplace=True)
    fastq_output_list = \
      results.to_dict(orient='records')
    return fastq_output_list
  except Exception as e:
    raise ValueError(
            'Failed to fetch fastq dir for sample id {0}, error: {1}'.\
              format(sample_igf_id_list,e))


def get_fastq_input_list(
      db_session_class,experiment_igf_id,combine_fastq_dir=False,
      fastq_collection_type='demultiplexed_fastq',active_status='ACTIVE'):
  '''
  A function for fetching all the fastq files linked to a specific experiment id
  
  :param db_session_class: A database session class
  :param experiment_igf_id: An experiment igf id
  :param fastq_collection_type: Fastq collection type name, default demultiplexed_fastq
  :param active_status: text label for active runs, default ACTIVE
  :param combine_fastq_dir: Combine fastq file directories for output line, default False
  :returns: A list of fastq file or fastq dir paths for the analysis run
  :raises ValueError: It raises ValueError if no fastq directory found
  '''
  try:
    fastq_output_list = []                                                        # default list of fastqs
    dbconnected = False
    base = BaseAdaptor(**{'session_class':db_session_class})
    base.start_session()
    dbconnected = True
    subquery = \
      base.session.\
        query(Run.run_igf_id).\
        join(Experiment, Experiment.experiment_id==Run.experiment_id).\
        filter(Run.experiment_id==Experiment.experiment_id).\
        filter(Experiment.experiment_igf_id==experiment_igf_id).\
        filter(Run.status==active_status)                                       # get subquery for run_igf_ids
    query = \
      base.session.\
        query(File.file_path).\
        join(Collection_group,File.file_id==Collection_group.file_id).\
        join(Collection,Collection.collection_id==Collection_group.collection_id).\
        filter(Collection.collection_id==Collection_group.collection_id).\
        filter(Collection_group.file_id==File.file_id).\
        filter(Collection.type==fastq_collection_type).\
        filter(Collection.name.in_(subquery))                                   # select file_path linked to run_igf_ids
    results = \
      base.fetch_records(
        query=query,
        output_mode='object')                                                   # fetch results as generator expression
    for row in results:
      if combine_fastq_dir:
        fastq_output_list.\
          append(os.path.dirname(row.file_path))                                # add fastq directory path to the output list
      else:
        fastq_output_list.\
          append(row.file_path)                                                 # add fastq files to the output list

    if len(fastq_output_list) > 0:
      fastq_output_list = \
        list(set(fastq_output_list))                                            # remove redundant values
    else:
      raise ValueError(
              'No fastq found for experiment {0}'.\
                format(experiment_igf_id))
    base.close_session()
    dbconnected = False
    return fastq_output_list                                                    # return list of fastq dirs
  except Exception as e:
    if dbconnected:
      base.close_session()
    raise ValueError(e)