import os,tarfile,subprocess
import pandas as pd
from igf_data.utils.fileutils import check_file_path,get_temp_dir,remove_dir,copy_local_file
from igf_data.utils.analysis_fastq_fetch_utils import get_fastq_input_list

def run_cellranger_multi(
      cellranger_exe,library_csv,sample_id,output_dir,use_ephemeral_space=False,
      job_timeout=43200,cellranger_options=('--localcores 1','--localmem 8')):
  try:
    tmp_dir = \
      get_temp_dir(use_ephemeral_space=use_ephemeral_space)
    os.chdir(tmp_dir)
    check_file_path(library_csv)
    check_file_path(cellranger_exe)
    check_file_path(output_dir)
    cmd = [
      cellranger_exe,
      'multi',
      '--id',sample_id,
      '--csv',library_csv,
      '--disable-ui']
    if isinstance(cellranger_options,tuple):
      cellranger_options = list(cellranger_options)
    if isinstance(cellranger_options,list) and \
       len(cellranger_options) > 0:
      cmd.extend(cellranger_options)
    cmd = \
      ' '.join(cmd)
    try:
      subprocess.\
        check_call(
          cmd,
          shell=True,
          timeout=job_timeout)
    except Exception as e:
      raise ValueError(
              'Failed to run cellranger multi, error: {0}'.\
                format(e))
    cellranger_output = \
      os.path.join(
        tmp_dir,
        sample_id,
        'outs')
    _check_cellranger_multi_output(
      cellranger_output,
      library_csv)
    output_dir = \
      os.path.join(output_dir,sample_id)
    copy_local_file(
      cellranger_output,
      output_dir)
    return cmd,output_dir
  except Exception as e:
    raise ValueError(
            'Failed cellranger run: error: {0}'.format(e))


def _check_cellranger_multi_output(cellranger_output, library_csv, sample_id):
  try:
    #file_check_dict = {
    #  'gene_expression':[
    #    'web_summary.html',
    #    'count/filtered_feature_bc_matrix.h5',
    #    'count/possorted_genome_bam.bam',
    #    'count/cloupe.cloupe'],
    #  'vdj':[
    #    'web_summary.html',
    #    'vdj/filtered_contig_annotations.csv',
    #    'vdj/vloupe.vloupe'],
    #  'vdj-b':[
    #    'web_summary.html',
    #    'vdj_b/filtered_contig_annotations.csv',
    #    'vdj_b/vloupe.vloupe'],
    #  'vdj-t':[
    #    'web_summary.html',
    #    'vdj_t/filtered_contig_annotations.csv',
    #    'vdj_t/vloupe.vloupe'],
    #  'antibody_capture':['web_summary.html'],
    #  'antigen_capture':['web_summary.html'],
    #  'crisper_guide_capture':['web_summary.html']}
    file_check_dict = {
      'gene_expression': [
        'per_sample_outs/{0}/web_summary.html'.format(sample_id),
        'per_sample_outs/{0}/metrics_summary.csv'.format(sample_id),
        'per_sample_outs/{0}/count/sample_feature_bc_matrix.h5'.format(sample_id),
        'per_sample_outs/{0}/count/sample_alignments.bam'.format(sample_id),
        'per_sample_outs/{0}/count/cloupe.cloupe'.format(sample_id)],
      'vdj': [
        'per_sample_outs/{0}/web_summary.html'.format(sample_id),
        'per_sample_outs/{0}/vdj/filtered_contig_annotations.csv'.format(sample_id),
        'per_sample_outs/{0}/vdj/vloupe.vloupe'.format(sample_id)],
      'vdj-b': [
        'per_sample_outs/{0}/web_summary.html'.format(sample_id),
        'per_sample_outs/{0}/vdj_b/filtered_contig_annotations.csv'.format(sample_id),
        'per_sample_outs/{0}/vdj_b/vloupe.vloupe'.format(sample_id)],
      'vdj-t': [
        'per_sample_outs/{0}/web_summary.html'.format(sample_id),
        'per_sample_outs/{0}/vdj_t/filtered_contig_annotations.csv'.format(sample_id),
        'per_sample_outs/{0}/vdj_t/vloupe.vloupe'.format(sample_id)],
      'antibody_capture': ['per_sample_outs/{0}/web_summary.html'.format(sample_id)],
      'antigen_capture': ['per_sample_outs/{0}/web_summary.html'.format(sample_id)],
      'crisper_guide_capture': ['per_sample_outs/{0}/web_summary.html'.format(sample_id)]}
    check_file_path(library_csv)
    data = list()
    columns = list()
    with open(library_csv,'r') as fp:
      lib_data = 0
      for line in fp:
        line = line.strip()
        if lib_data == 1:
          if line.startswith('fastq_id'):
            columns = line.split(',')
          else:
            data.extend([line.split(',')])
        if line.startswith('[libraries]'):
          lib_data = 1
    df = pd.DataFrame(data,columns=columns)
    feature_list = \
      list(df['feature_types'].\
           map(lambda x: x.lower().replace(' ','_')).\
           drop_duplicates().values)
    for f in feature_list:
      if f not in file_check_dict:
        raise KeyError('Missing feature {0}'.format(f))
      file_list = file_check_dict.get(f)
      if file_list is None:
        raise ValueError(
                'No file entry found for feature {0}'.\
                  format(f))
      for file_path in file_list:
        check_file_path(
          os.path.join(cellranger_output,file_path))
  except Exception as e:
    raise ValueError(
            'Failed cellranger output checking, error: {0}'.\
              format(e))


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
                                  file_list=('web_summary.html',
                                             'metrics_summary.csv',
                                             'possorted_genome_bam.bam',
                                             'possorted_genome_bam.bam.bai',
                                             'filtered_feature_bc_matrix.h5',
                                             'raw_feature_bc_matrix.h5',
                                             'molecule_info.h5',
                                             'cloupe.cloupe',
                                             'analysis/tsne/2_components/projection.csv',
                                             'analysis/clustering/graphclust/clusters.csv',
                                             'analysis/diffexp/kmeans_3_clusters/differential_expression.csv',
                                             'analysis/pca/10_components/variance.csv'
                                            )):
  '''
  A function for checking cellranger count output
  
  :param output_path: A filepath for cellranger count output directory
  :param file_list: List of files to check in the output directory
    
                      default file list to check
                        web_summary.html
                        metrics_summary.csv
                        possorted_genome_bam.bam
                        possorted_genome_bam.bam.bai
                        filtered_feature_bc_matrix.h5
                        raw_feature_bc_matrix.h5
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
    file_list = list(file_list)
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
                                             collection_name=None,
                                             collection_type=None,
                                             attribute_name='attribute_name',
                                             attribute_value='attribute_value',
                                             attribute_prefix='None',
                                             target_filename='metrics_summary.csv'):
  '''
  A function for extracting metrics summary file for cellranger ourput tar and parse the file.
  Optionally it can add the collection name and type info to the output dictionary.
  
  :param cellranger_tar: A cellranger output tar file
  :param target_filename: A filename for metrics summary file lookup, default metrics_summary.csv
  :param collection_name: Optional collection name, default None
  :param collection_type: Optional collection type, default None
  :param attribute_tag: An optional string to add as prefix of the attribute names, default None
  :returns: A dictionary containing the metrics values
  '''
  try:
    check_file_path(cellranger_tar)
    temp_work_dir = get_temp_dir(use_ephemeral_space=False)
    metrics_file = None
    with tarfile.open(cellranger_tar,mode='r') as tar:
      for file_name in tar.getnames():
        if os.path.basename(file_name) == target_filename:
            tar.extract(file_name,path=temp_work_dir)
            metrics_file = os.path.join(temp_work_dir,
                                        file_name)

    if metrics_file is None:
      raise IOError('Required file {0} not found in tar {1}'.\
                    format(target_filename,cellranger_tar))

    attribute_data = pd.read_csv(metrics_file).T.\
                     reset_index()
    attribute_data.columns = [attribute_name,attribute_value]
    if attribute_prefix is None:
      attribute_data[attribute_name] = \
        attribute_data[attribute_name].\
          map(lambda x: x.replace(' ','_'))
    else:
      attribute_data[attribute_name] = \
        attribute_data[attribute_name].\
          map(lambda x: \
              '{0}_{1}'.format(\
                attribute_prefix,
                x.replace(' ','_')))

    if collection_name is not None:
      attribute_data['name'] = collection_name
    if collection_type is not None:
      attribute_data['type'] = collection_type

    attribute_data = attribute_data.\
                     to_dict(orient='records')
    remove_dir(temp_work_dir)
    return attribute_data
  except:
    raise


