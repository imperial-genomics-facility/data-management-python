import os,logging,subprocess,re
import pandas as pd
from airflow.models import Variable
from igf_airflow.logging.upload_log_msg import log_success,log_failure,log_sleep
from igf_airflow.logging.upload_log_msg import post_image_to_channels
from igf_data.utils.fileutils import get_temp_dir,copy_remote_file,check_file_path,read_json_data
from igf_data.utils.singularity_run_wrapper import execute_singuarity_cmd
from igf_data.utils.analysis_fastq_fetch_utils import get_fastq_and_run_for_samples
from igf_data.utils.fileutils import get_temp_dir
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor

## FUNCTION
def configure_cellranger_run_func(**context):
  try:
    # xcop_pull analysis_description
    # xcom_pull analysis_info
    # fetch reference genomes, or
    # accept overrides for reference genomes from analysis_description
    # generate library.csv file for cellranger run
    # push the csv path to xcom
    pass
  except Exception as e:
    logging.error(e)
    raise ValueError(e)


def fetch_analysis_info_and_branch_func(**context):
  """
  Fetch dag_run.conf and analysis_description
  """
  try:
    analysis_list = list()
    dag_run = context.get('dag_run')
    ti = context.get('ti')
    no_analysis = context['params'].get('no_analysis_task')
    database_config_file = Variable.get('database_config_file')
    analysis_list.append(no_analysis)
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('analysis_description') is not None:
      analysis_description = \
        dag_run.conf.get('analysis_description')
      analysis_id = \
        dag_run.conf.get('analysis_id')
      analysis_type = \
        dag_run.conf.get('analysis_type')
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
          combine_fastq_dir=False)
      analysis_info = \
        _fetch_formatted_analysis_description(analysis_description,fastq_list)
      if len(messages) > 0:
        raise ValueError('Analysis description formatting failed: {0}'.\
                format(messages))
      # mark analysis_id as running,if its not already running
      status = _check_and_mark_analysis_seed_running(
        analysis_id=analysis_id,
        anslysis_type=analysis_type,
        database_config_file=database_config_file)
      # xcom push analysis_info and analysis_description
      if status:
        ti.xcom_push(key='analysis_description',value=analysis_description)
        ti.xcom_push(key='analysis_info',value=analysis_info)
      else:
        analysis_list = [no_analysis]
    return analysis_list
  except Exception as e:
    logging.error(e)
    raise ValueError(e)


def _check_and_mark_analysis_seed_running(
      analysis_id,anslysis_type,database_config_file,
      running_status='RUNNING',analysis_table='analysis'):
  try:
    dbparam = \
      read_dbconf_json(database_config_file)
    pl = \
      PipelineAdaptor(**dbparam)
    status = \
      pl.create_or_update_pipeline_seed(
        seed_id=analysis_id,
        pipeline_name=anslysis_type,
        new_status=running_status,
        seed_table=analysis_table,
        no_change_status=running_status)
    return status
  except Exception as e:
    raise ValueError(e)

def _fetch_formatted_analysis_description(
      analysis_description,fastq_run_list,feature_column='feature_type',
      sample_column='sample_igf_id',run_column='run_igf_id',file_column='file_path'):
  try:
    formatted_analysis_description = dict()
    analysis_description_df = pd.DataFrame(analysis_description)
    fastq_run_list_df = pd.DataFrame(fastq_run_list)
    fastq_run_list_df['fastq_dir'] = \
      fastq_run_list_df[file_column].\
        map(lambda x: os.path.dirname(x))
    tmp_dir = get_temp_dir(use_ephemeral_space=True)
    for feature,f_data in analysis_description_df.groupby(feature_column):
      feature = \
        feature.replace(' ','_').\
        lower()
      sample_igf_id = \
        list(f_data[sample_column].values)[0]
      sample_records = \
        fastq_run_list_df[fastq_run_list_df[sample_column]==sample_igf_id]
      if len(sample_records.index)==0:
        raise ValueError(
                'No records found for sample: {0}, feature: {1}'.\
                  format(sample_igf_id,feature))
      total_runs_for_sample = \
        len(list(
          sample_records[run_column].\
            drop_duplicates().\
            values))
      fastq_file_name = \
        list(sample_records[file_column].values)[0]
      file_name_pattern = \
        re.compile(r'(\S+)_S\d+_L00\d_(R|I)(\d)_001\.fastq\.gz')
      sample_prefix_match = \
        re.match(
          file_name_pattern,
          os.path.basename(fastq_file_name))
      if sample_prefix_match is None:
        raise ValueError(
                'Failed to match fastq file for {0}'.\
                  format(fastq_file_name))
      sample_prefix = sample_prefix_match.groups()[0]
      sample_records = \
        sample_records[[run_column,'fastq_dir']].\
          drop_duplicates()
      sample_records = \
        sample_records.\
          to_dict(orient='records')
      formatted_run_records = dict()
      for i,run in enumerate(sample_records):
        run_igf_id = run.get(run_column)
        fastq_dir = run.get('fastq_dir')
        tmp_output_path = \
          os.path.join(tmp_dir,feature,sample_igf_id,run_igf_id)
        if not os.path.exists(tmp_output_path):
          os.makedirs(tmp_output_path)
        formatted_run_records.\
          update({
            str(i):{
              "run_igf_id":run_igf_id,
              "fastq_dir":fastq_dir,
              "output_path":tmp_output_path
            }})
      formatted_analysis_description.\
        update({
          feature:{
            'sample_igf_id':sample_igf_id,
            'sample_name':sample_prefix,
            'run_count':total_runs_for_sample,
            'runs':formatted_run_records
          }})
    return formatted_analysis_description
  except Exception as e:
    raise ValueError(e)

def _validate_analysis_description(
      analysis_description,feature_types,sample_column='sample_igf_id',
      feature_column='feature_type',reference_column='reference'):
  try:
    messages = list()
    analysis_list = list()
    sample_id_list = list()
    
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
  except Exception as e:
    raise ValueError(e)
