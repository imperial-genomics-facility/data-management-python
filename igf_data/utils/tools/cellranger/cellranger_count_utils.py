import os,tarfile
import pandas as pd
from igf_data.utils.fileutils import check_file_path,get_temp_dir,remove_dir
from igf_data.utils.analysis_fastq_fetch_utils import get_fastq_input_list

def get_cellranger_count_input_list(db_session_class,experiment_igf_id,
                                    fastq_collection_type='demultiplexed_fastq',
                                    active_status='ACTIVE'):
  '''
  A function for fetching input list for cellranger count run for a specific experiment
  
  :param db_session_class: A database session class
  :param experiment_igf_id: An experiment igf id
  :param fastq_collection_type: Fastq collection type name, default demultiplexed_fastq
  :param active_status: text label for active runs, default ACTIVE
  :returns: A list of fastq dir path for the cellranger count run
  :raises ValueError: It raises ValueError if no fastq directory found
  '''
  try:
    fastq_dir_list=get_fastq_input_list(db_session_class=db_session_class,
                                        experiment_igf_id=experiment_igf_id,
                                        combine_fastq_dir=True,
                                        fastq_collection_type='demultiplexed_fastq',
                                        active_status='ACTIVE')                 # get fastq dir list
    return fastq_dir_list                                                       # return list of fastq dirs
  except:
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


def extract_cellranger_count_metrics_summary(cellranger_tar,
                                             target_filename='metrics_summary.csv'):
  '''
  A function for extracting metrics summary file for cellranger ourput tar and parse the file
  
  :param cellranger_tar: A cellranger output tar file
  :param target_filename: A filename for metrics summary file lookup, default metrics_summary.csv
  '''
  try:
    check_file_path(cellranger_tar)
    temp_work_dir=get_temp_dir(use_ephemeral_space=True)
    metrics_file=None
    with tarfile.open(cellranger_tar,mode='r') as tar:
      for file_name in tar.getnames():
        if os.path.basename(file_name) == target_filename:
            tar.extract(file_name,path=temp_work_dir)
            metrics_file=os.path.join(temp_work_dir,
                                      file_name)

    if metrics_file is None:
      raise IOError('Required file {0} not found in tar {1}'.\
                    format(target_filename,cellranger_tar))

    attribute_data=pd.read_csv(metrics_file).\
                   to_dict(orient='records')
    remove_dir(temp_work_dir)
    return attribute_data
  except:
    raise


