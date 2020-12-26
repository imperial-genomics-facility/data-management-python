import os,logging,subprocess,re,fnmatch
import pandas as pd
from copy import copy
from airflow.models import Variable
from igf_airflow.logging.upload_log_msg import log_success,log_failure,log_sleep
from igf_airflow.logging.upload_log_msg import post_image_to_channels
from igf_data.utils.fileutils import get_temp_dir,copy_remote_file,check_file_path
from igf_data.utils.fileutils import read_json_data,copy_local_file
from igf_data.utils.singularity_run_wrapper import execute_singuarity_cmd
from igf_data.utils.analysis_fastq_fetch_utils import get_fastq_and_run_for_samples
from igf_data.utils.fileutils import get_temp_dir
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.utils.tools.reference_genome_utils import Reference_genome_utils
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.utils.tools.cutadapt_utils import run_cutadapt
from igf_data.utils.tools.cellranger.cellranger_count_utils import run_cellranger_multi

## FUNCTION
def run_cellranger_tool(**context):
  try:
    ti = context.get('ti')
    analysis_description_xcom_pull_task = \
      context['params'].get('analysis_description_xcom_pull_task')
    analysis_description_xcom_key = \
      context['params'].get('analysis_description_xcom_key')
    library_csv_xcom_key = \
      context['params'].get('library_csv_xcom_key')
    library_csv_xcom_pull_task = \
      context['params'].get('library_csv_xcom_pull_task')
    cellranger_xcom_key = \
      context['params'].get('cellranger_xcom_key')
    analysis_description = \
      ti.xcom_pull(
        task_id=analysis_description_xcom_pull_task,
        key=analysis_description_xcom_key)
    library_csv = \
      ti.xcom_pull(
        task_id=library_csv_xcom_pull_task,
        key=library_csv_xcom_key)
    cellranger_exe = Variable.get('cellranger_exe')
    job_timeout = Variable.get('cellranger_job_timeout')
    cellranger_options = context['params'].get('cellranger_options')
    output_dir = get_temp_dir(use_ephemeral_space=True)
    sample_id = None
    for entry in analysis_description:
      sample_igf_id = entry.get('sample_igf_id')
      if sample_id is None:
        sample_id = sample_igf_id
      else:
        sample_id = '{0}_{1}'.format(sample_id,sample_igf_id)
    c = \
      run_cellranger_multi(
        cellranger_exe=cellranger_exe,
        library_csv=library_csv,
        sample_id=sample_id,
        output_dir=output_dir,
        use_ephemeral_space=False,
        job_timeout=job_timeout,
        cellranger_options=cellranger_options)
    ti.xcom_push(
      key=cellranger_xcom_key,
      value=output_dir)
  except Exception as e:
    logging.error(e)
    raise ValueError(e)


def run_sc_read_trimmming_func(**context):
  try:
    ti = context.get('ti')
    xcom_pull_task_id = \
      context['params'].get('xcom_pull_task_id')
    analysis_info_xcom_key = \
      context['params'].get('analysis_info_xcom_key')
    analysis_name = \
      context['params'].get('analysis_name')
    run_id = \
      context['params'].get('run_id')
    r1_length = \
      context['params'].get('r1_length')
    r2_length = \
      context['params'].get('r2_length')
    fastq_input_dir_tag = \
      context['params'].get('fastq_input_dir_tag')
    fastq_output_dir_tag = \
      context['params'].get('fastq_output_dir_tag')
    singularity_image = \
      Variable.get('cutadapt_singularity_image')
    analysis_info = \
      ti.xcom_pull(
        task_id=xcom_pull_task_id,
        key=analysis_info_xcom_key)
    _get_fastq_and_run_cutadapt_trim(
      analysis_info= analysis_info,
      analysis_name=analysis_name,
      run_id=run_id,
      r1_length=r1_length,
      r2_length=r2_length,
      fastq_input_dir_tag=fastq_input_dir_tag,
      fastq_output_dir_tag=fastq_output_dir_tag,
      singularity_image=singularity_image)
  except Exception as e:
    logging.error(e)
    raise ValueError(e)


def _get_fastq_and_run_cutadapt_trim(
      analysis_info,analysis_name,run_id,r1_length,
      r2_length,fastq_input_dir_tag,fastq_output_dir_tag,
      singularity_image,cutadapt_exe='cutadapt',dry_run=False,
      cutadapt_options=('--cores=1',)):
  """
  An internal method for trimming or copying fastq files for Cellranger run

  :param analysis_info: Analysis info dictionary object
  :param analysis_name: Analysis name to fetch data from analysis info
  :param run_id: Run id to fetch fastq paths from analysis info
  :param r1_length: Trimmed length for R1 fastq
  :param r2_length: Trimmed length for R2 fastq
  :param fastq_input_dir_tag: Fastq input dir tag for analysis info lookup
  :param fastq_output_dir_tag: Fastq output dir tag for analysis info lookup
  :param singularity_image: Singularity image path for cutadapt tool
  :param cutadapt_exe: Cutadapt exe path, default cutadapt
  :param dry_run: A toggle for dry run, default False
  :param cutadapt_options: Cutadapt run options, default ('--cores=1',)
  :returns: None
  """
  try:
    sample_info = \
      analysis_info.get(analysis_name)
    if sample_info is None:
      raise ValueError(
              'No feature {0} found in analysis_info'.\
                format(analysis_info))
    run = sample_info.get('runs').get(str(run_id))
    if run is None:
      raise ValueError(
              'No run {0} found for feature {1} in analysis_info'.\
                format(run,analysis_name))
    if isinstance(cutadapt_options,tuple):
      cutadapt_options = list(cutadapt_options)
    input_fastq_dir = run.get(fastq_input_dir_tag)
    output_fastq_dir = run.get(fastq_output_dir_tag)
    r1_file_name_pattern = \
      re.compile(r'(\S+)_S\d+_L00\d_R1_001\.fastq\.gz')
    r2_file_name_pattern = \
      re.compile(r'(\S+)_S\d+_L00\d_R2_001\.fastq\.gz')
    index_file_name_pattern = \
      re.compile(r'(\S+)_S\d+_L00\d_I(\d)_001\.fastq\.gz')
    for fastq in os.listdir(input_fastq_dir):
      if fnmatch.fnmatch(fastq,'*.fastq.gz'):
        input_fastq_file = \
          os.path.join(input_fastq_dir,fastq)
        output_fastq_file = \
          os.path.join(output_fastq_dir,fastq)
        if re.match(r1_file_name_pattern,fastq):
          # trim R1
          if r1_length > 0:
            cutadapt_options_r1 = None
            if len(cutadapt_options) >0 :
              cutadapt_options_r1 = copy(cutadapt_options)
              cutadapt_options_r1.\
                append('-l {0}'.format(r1_length))
            c = \
              run_cutadapt(
                read1_fastq_in=input_fastq_file,
                read1_fastq_out=output_fastq_file,
                cutadapt_options=cutadapt_options_r1,
                cutadapt_exe=cutadapt_exe,
                dry_run=dry_run,
                singularity_image_path=singularity_image)
          else:
            copy_local_file(
              input_fastq_file,
              output_fastq_file)
        if re.match(r2_file_name_pattern,fastq):
          # trim R2
          if r2_length > 0:
            cutadapt_options_r2 = None
            if len(cutadapt_options) >0 :
              cutadapt_options_r2 = copy(cutadapt_options)
              cutadapt_options_r2.\
                append('-l {0}'.format(r2_length))
            _ = \
              run_cutadapt(
                read1_fastq_in=input_fastq_file,
                read1_fastq_out=output_fastq_file,
                cutadapt_options=cutadapt_options_r2,
                cutadapt_exe=cutadapt_exe,
                dry_run=dry_run,
                singularity_image_path=singularity_image)
          else:
            copy_local_file(
              input_fastq_file,
              output_fastq_file)
        if re.match(index_file_name_pattern,fastq):
          # copy I1 or I2
          copy_local_file(
            input_fastq_file,
            output_fastq_file)
  except Exception as e:
    raise ValueError(
            'Failed to trim or copy reads, error: {0}'.\
              format(e))


def configure_cellranger_run_func(**context):
  try:
    ti = context.get('ti')
    # xcop_pull analysis_description
    # xcom_pull analysis_info
    xcom_pull_task_id = \
      context['params'].get('xcom_pull_task_id')
    analysis_description_xcom_key = \
      context['params'].get('analysis_description_xcom_key')
    analysis_info_xcom_key = \
      context['params'].get('analysis_info_xcom_key')
    library_csv_xcom_key = \
      context['params'].get('library_csv_xcom_key')
    work_dir = \
      get_temp_dir(use_ephemeral_space=True)
    analysis_description = \
      ti.xcom_pull(
        task_id=xcom_pull_task_id,
        key=analysis_description_xcom_key)
    analysis_info = \
      ti.xcom_pull(
        task_id=xcom_pull_task_id,
        key=analysis_info_xcom_key)
    # generate library.csv file for cellranger run
    csv_path = \
      _create_library_csv_for_cellranger_multi(
        analysis_description=analysis_description,
        analysis_info=analysis_info,
        work_dir=work_dir)
    # push the csv path to xcom
    ti.xcom_push(
      key=library_csv_xcom_key,
      value=csv_path)
  except Exception as e:
    logging.error(e)
    raise ValueError(e)


def _get_data_for_csv_header_section(analysis_entry,column_list):
  try:
    data = dict()
    for col in column_list:
      if col in analysis_entry and \
         analysis_entry.get(col) is not None:
        data.update({col:analysis_entry.get(col)})
    return data
  except:
    raise

def _create_library_csv_for_cellranger_multi(analysis_description,analysis_info,work_dir):
  try:
    header_section_columns = {
      'gene-expression' : [
        'reference',
        'target-panel',
        'no-target-umi-filter',
        'r1-length',
        'r2-length',
        'chemistry',
        'expect-cells',
        'force-cells',
        'include-introns'],
      'feature':[
        'reference',
        'r1-length',
        'r2-length'],
      'vdj':[
        'reference',
        'inner-enrichment-primers',
        'r1-length',
        'r2-length']}
    library_cols = [
      'fastq_id',
      'fastqs',
      'lanes',
      'feature_types',
      'subsample_rate']
    header_data = dict()
    for analysis_entry in analysis_description:
      feature_type = analysis_entry.get('feature_type')
      feature_type = feature_type.replace(' ','_').lower()
      header_section = None
      if feature_type == 'gene_expression':
        header_section = 'gene-expression'
      elif feature_type in ('vdj','vdj-t','vdj-b'):
        header_section = 'vdj'
      elif feature_type in ("antibody_capture","antigen_capture","crisper_guide_capture"):
        header_section = 'feature'
      column_list = header_section_columns.get(header_section)
      data = \
        _get_data_for_csv_header_section(
          analysis_entry,
          column_list)
      if header_section is None:
        raise ValueError(
                'Failed to get header section for {0}'.\
                  format(analysis_entry))
      header_data.\
        update({header_section:data})
    library_data = list()
    for feature_name,feature_data in analysis_info.items():
      fastq_id = feature_data.get('sample_name')
      feature_name = feature_name.replace('_',' ')
      runs = feature_data.get('runs')
      for _,run_data in runs.items():
        fastqs = run_data.get('output_path')
        if fastq_id is None or \
           feature_name is None or \
           fastqs is None:
          raise ValueError(
                  'Misssing data: {0}, {1}'.\
                    format(feature_name,feature_data))
        library_data.\
          append({
            'fastq_id':fastq_id,
            'fastqs':fastqs,
            'lanes':'',
            'feature_types':feature_name,
            'subsample_rate':''
          })
    output_csv = os.path.join(work_dir,'library.csv')
    with open(output_csv,'w') as fp:
      for header_key,data in header_data.items():
        fp.write('[{0}]\n'.format(header_key))
        for data_key,data_value in data.items():
          fp.write('{0},{1}\n'.format(data_key,data_value))
      fp.write('[libraries]\n')
      fp.write('{0}\n'.format(','.join(library_cols)))
      for entry in library_data:
        data_col = [
          entry.get(col)
            for col in library_cols]
        fp.write('{0}\n'.format(','.join(data_col)))
    return output_csv
  except Exception as e:
    raise ValueError(
            'Failed to create cellranger config file for run, error: {0}'.\
              format(e))


def fetch_analysis_info_and_branch_func(**context):
  """
  Fetch dag_run.conf and analysis_description
  """
  try:
    analysis_list = list()
    dag_run = context.get('dag_run')
    ti = context.get('ti')
    no_analysis = context['params'].get('no_analysis_task')
    analysis_description_xcom_key = context['params'].get('analysis_description_xcom_key')
    analysis_info_xcom_key = context['params'].get('analysis_info_xcom_key')
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
      # add reference genome paths if reference type and genome build is present
      # check for genome build info
      analysis_description = \
        _add_reference_genome_path_for_analysis(
          database_config_file=database_config_file,
          analysis_description=analysis_description,
          genome_required=True)
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
        ti.xcom_push(key=analysis_description_xcom_key,value=analysis_description)
        ti.xcom_push(key=analysis_info_xcom_key,value=analysis_info)
      else:
        analysis_list = [no_analysis]
    return analysis_list
  except Exception as e:
    logging.error(e)
    raise ValueError(e)


def _add_reference_genome_path_for_analysis(
      database_config_file,analysis_description,genome_column='genome_build',
      genome_required=True,reference_column='reference',reference_type_column='reference_type'):
  try:
    dbparams = read_dbconf_json(database_config_file)
    base = BaseAdaptor(**dbparams)
    messages = list()
    for entry in analysis_description:
      reference = entry.get(reference_column)
      genome_build = entry.get(genome_column)
      reference_type = entry.get(reference_type_column)
      if genome_build is None and \
         genome_required:
        raise ValueError(
                'No genome build info present: {0}'.\
                  format(entry))
      if reference is None:
        if genome_build is None or \
           reference_type is None:
           messages.\
             append('No reference infor for entry {0}'.\
                format(entry))
        else:
          ref_tool = \
            Reference_genome_utils(
              dbsession_class=base.get_session_class(),
              genome_tag=genome_build)
          reference = \
            ref_tool.\
              _fetch_collection_files(
                collection_type=reference_type,
                check_missing=False)
          if reference is None:
            messages.\
              append('Failed to fetch ref from db for entry {0}'.\
                format(entry))
          entry.\
            update({
              reference_column:reference})
    if len(messages) > 0:
      raise ValueError(
              'List of errors for reference lookup: {0}'.\
                format(messages))
    return analysis_description
  except Exception as e:
    raise ValueError(
            'Failed to fetch reference genome, error: {0}'.\
              format(e))


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
