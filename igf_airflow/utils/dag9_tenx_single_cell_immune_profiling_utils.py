import os,logging,subprocess
import pandas as pd
from airflow.models import Variable
from igf_airflow.logging.upload_log_msg import log_success,log_failure,log_sleep
from igf_airflow.logging.upload_log_msg import post_image_to_channels
from igf_data.utils.fileutils import get_temp_dir,copy_remote_file,check_file_path,read_json_data
from igf_data.utils.singularity_run_wrapper import execute_singuarity_cmd
from igf_data.utils.analysis_fastq_fetch_utils import get_fastq_and_run_for_samples

## FUNCTION
def fetch_analysis_info_and_branch_func(**context):
  """
  Fetch dag_run.conf and analysis_description
  """
  analysis_list = list()
  dag_run = context.get('dag_run')
  no_analysis = context.get('no_analysis_task')
  database_config_file = Variable.get('database_config_file')
  analysis_list.append(no_analysis)
  if dag_run is not None and \
     dag_run.conf is not None and \
     dag_run.conf.get('analysis_description') is not None:
    analysis_description = \
      dag_run.conf.get('analysis_description')
    feature_types = \
      Variable.get('tenx_single_cell_immune_profiling_feature_types')
    # check the analysis description and sample validity
    # warn if multiple samples are allocated to same sub category
    # filter analysis branch list
    sample_id_list, analysis_list, messages = \
      _validate_analysis_description(
        analysis_description=analysis_description,
        feature_types=feature_types)
    if len(messages) > 0:
      raise ValueError('Analysis validation failed: {0}'.\
              format(messages))
    # get the fastq paths for sample ids and set the trim output dirs per run
    fastq_list = \
      get_fastq_and_run_for_samples(
        dbconfig_file=database_config_file,
        sample_igf_id_list=sample_id_list,
        active_status='ACTIVE',
        combine_fastq_dir=True)
    df = pd.DataFrame(fastq_list)
    df_sample_ids = \
      list(df['sample_igf_id'].drop_duplicates().values)
    if len(sample_id_list) != len(df_sample_ids):
      raise ValueError(
              'Expecting samples: {0}, received samples: {1}'.\
                format(sample_id_list,df_sample_ids))
    """
    gene_expression : {
    sample_id: IGF_ID,
    sample_name: submitter_id,
    runs: [{
      run_id: run1,
      fastq_dir: /path/lane1/sample1,
      trim_output_dirs: /path/lane1/sample1
    },{
      run_id: run2,
      fastq_dir: /path/lane1/sample1,
      trim_output_dirs: /path/lane1/sample1 
    },{
      run_id: run3,
      fastq_dir: /path/lane1/sample1,
      trim_output_dirs: /path/lane1/sample1 
    }]
  }
    """
    # check and mark analysis as running in the pipeline seed table, to avoid conflict
  
  return analysis_list

def _validate_analysis_description(analysis_description,feature_types):
  messages = list()
  analysis_list = list()
  sample_id_list = list()
  sample_column = 'sample_igf_id'
  feature_column = 'feature_type'
  reference_column = 'reference'
  if not isinstance(analysis_description,list):
    raise ValueError(
            'Expecting a list of analysis_description, got {0}'.\
              format(type(analysis_description)))
  if not isinstance(feature_types,list):
    raise ValueError(
            'Expecting a list for feature_types, got {0}'.\
              format(type(feature_types)))
  df = pd.DataFrame(analysis_description)
  for c in (sample_column,feature_column):
    if c not in df.columns:
      messages.\
        append('missing {0} in analysis_data'.format(c))
  analysis_list = \
    list(
      df[feature_column].\
        dropna().\
        drop_duplicates().\
        values)
  analysis_list = \
    set(
      [f.replace(' ','_').lower()
        for f in analysis_list])
  sample_id_list = \
    list(
      df[sample_column].\
        dropna().\
        drop_duplicates().\
        values)
  for f,f_data in df.groupby(feature_column):
    f = f.replace(' ','_').lower()
    f_samples = list(f_data[sample_column].values)
    if f not in feature_types:
      messages.\
        append('feature_type {0} in not defined: {1}'.\
                 format(f,f_samples))
    if len(f_samples) > 1:
      messages.\
        append('feature {0} has {1} samples: {2}'.\
                 format(f,len(f_samples),','.join(f_samples)))
  if reference_column in df.columns:
    ref_msg = \
      ['reference {0} does not exists'.format(r)
        for r in list(df['reference'].dropna().values)
          if not os.path.exists(r)]
    if len(ref_msg) > 0:
      messages.\
        extend(ref_msg)
  return sample_id_list, analysis_list, messages
