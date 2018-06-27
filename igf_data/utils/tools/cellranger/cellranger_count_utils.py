import os
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.experimentadaptor import ExperimentAdaptor
from igf_data.igfdb.igfTables import File,Experiment,Run,Collection,Collection_group


def get_cellranger_count_input_list(db_session_class,experiment_igf_id,
                                    fastq_collection_type='demultiplexed_fastq'):
  '''
  A function for fetching input list for cellranger count run for a specific experiment
  
  :param db_session_class: A database session class
  :param experiment_igf_id: An experiment igf id
  :param fastq_collection_type: Fastq collection type name, default demultiplexed_fastq
  :returns: A list of fastq dir path for the cellranger count run
  :raises ValueError: It raises ValueError if no fastq directory found
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
             filter(Experiment.experiment_igf_id==experiment_igf_id).\
             filter(Run.status=='ACTIVE')                                       # get subquery for run_igf_ids
    query=base.session.\
          query(File.file_path).\
          join(Collection_group).\
          join(Collection).\
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
    else:
      raise ValueError('No fastq directory found for experiment {0}'.\
                       format(experiment_igf_id))

    base.close_session()
    return fastq_dir_list                                                       # return list of fastq dirs
  except:
    if dbconnected:
      base.close_session()
    raise

def check_cellranger_count_output(output_path,
                                  file_list=['web_summary.html',
                                             'metrics_summary.csv',
                                             'possorted_genome_bam.bam',
                                             'possorted_genome_bam.bam.bai',
                                             'filtered_gene_bc_matrices_h5.h5',
                                             'raw_gene_bc_matrices_h5.h5',
                                             'molecule_info.h5',
                                             'cloupe.cloupe',
                                             'analysis/tsne/2_components/projection.csv',
                                             'analysis/clustering/graphclust/clusters.csv',
                                             'analysis/diffexp/kmeans_3_clusters/differential_expression.csv',
                                             'analysis/pca/10_components/variance.csv'
                                            ]):
  '''
  A function for checking cellranger count output
  
  :param output_path: A filepath for cellranger count output directory
  :param file_list: List of files to check in the output directory
    
                      default file list to check
                        web_summary.html
                        metrics_summary.csv
                        possorted_genome_bam.bam
                        possorted_genome_bam.bam.bai
                        filtered_gene_bc_matrices_h5.h5
                        raw_gene_bc_matrices_h5.h5
                        molecule_info.h5
                        cloupe.cloupe
                        analysis/tsne/2_components/projection.csv
                        analysis/clustering/graphclust/clusters.csv
                        analysis/diffexp/kmeans_3_clusters/differential_expression.csv
                        analysis/pca/10_components/variance.csv
  :returns: Nill
  :raises IOError: when any file is missing from the output path
  '''
  try:
    file_dict_value=[i for i in range(len(file_list))]                          # get file index values
    file_dict=dict(zip(file_list,file_dict_value))                              # convert file list to a dictionary
    for file_name in file_list:
      file_path=os.path.join(output_path,
                             file_name)
      if os.path.exists(file_path) and \
         os.path.getsize(file_path) > 0:
        del file_dict[file_name]                                                # remove file from dictionary if its present

    if len(list(file_dict.keys())) > 0:
      raise IOError('Partial Cellranger count output found, missing following files: {0}'.\
                    format(list(file_dict.keys())))

  except:
    raise
