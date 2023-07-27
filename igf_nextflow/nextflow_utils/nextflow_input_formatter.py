import os
import re
import pandas as pd
from typing import Tuple
from igf_data.utils.fileutils import check_file_path
from igf_data.utils.fileutils import get_temp_dir
from igf_data.utils.analysis_fastq_fetch_utils import get_fastq_and_run_for_samples
from igf_airflow.utils.dag22_bclconvert_demult_utils import _create_output_from_jinja_template

def prepare_input_for_multiple_nfcore_pipeline(
      runner_template_file: str,
      config_template_file: str,
      project_name: str,
      hpc_data_dir: str,
      dbconf_file: str,
      sample_metadata: dict,
      analysis_metadata: dict,
      nfcore_pipeline_name: str,
      supported_nfcore_pipelines: list = [
        'nf-core/smrnaseq',
        'nf-core/rnaseq',
        'nf-core/methylseq',
        'nf-core/sarek',
        'nf-core/ampliseq',
        'nf-core/rnafusion',
        'nf-core/rnavar',
        'nf-core/atacseq',
        'nf-core/chipseq',
        'nf-core/cutandrun',
        'nf-core/bactmap',
        'nf-core/hic'
      ]) -> \
        Tuple[str, str]:
  try:
    if nfcore_pipeline_name not in supported_nfcore_pipelines:
      raise KeyError(
        f"NFCore pipeline {nfcore_pipeline_name} is not supported yet")
    work_dir = None
    runner_file = None
    if nfcore_pipeline_name == 'nf-core/smrnaseq':
      work_dir, runner_file = \
        prepare_nfcore_smrnaseq_input(
          runner_template_file=runner_template_file,
          config_template_file=config_template_file,
          project_name=project_name,
          hpc_data_dir=hpc_data_dir,
          dbconf_file=dbconf_file,
          sample_metadata=sample_metadata,
          analysis_metadata=analysis_metadata,
          nfcore_pipeline_name=nfcore_pipeline_name)
    if nfcore_pipeline_name == 'nf-core/rnaseq':
      work_dir, runner_file = \
        prepare_nfcore_rnaseq_input(
          runner_template_file=runner_template_file,
          config_template_file=config_template_file,
          project_name=project_name,
          hpc_data_dir=hpc_data_dir,
          dbconf_file=dbconf_file,
          sample_metadata=sample_metadata,
          analysis_metadata=analysis_metadata,
          nfcore_pipeline_name=nfcore_pipeline_name)
    if nfcore_pipeline_name == 'nf-core/methylseq':
      work_dir, runner_file = \
        prepare_nfcore_methylseq_input(
          runner_template_file=runner_template_file,
          config_template_file=config_template_file,
          project_name=project_name,
          hpc_data_dir=hpc_data_dir,
          dbconf_file=dbconf_file,
          sample_metadata=sample_metadata,
          analysis_metadata=analysis_metadata,
          nfcore_pipeline_name=nfcore_pipeline_name)
    if nfcore_pipeline_name == 'nf-core/sarek':
      work_dir, runner_file = \
        prepare_nfcore_sarek_input(
          runner_template_file=runner_template_file,
          config_template_file=config_template_file,
          project_name=project_name,
          hpc_data_dir=hpc_data_dir,
          dbconf_file=dbconf_file,
          sample_metadata=sample_metadata,
          analysis_metadata=analysis_metadata,
          nfcore_pipeline_name=nfcore_pipeline_name)
    if nfcore_pipeline_name == 'nf-core/ampliseq':
      work_dir, runner_file = \
        prepare_nfcore_ampliseq_input(
          runner_template_file=runner_template_file,
          config_template_file=config_template_file,
          project_name=project_name,
          hpc_data_dir=hpc_data_dir,
          dbconf_file=dbconf_file,
          sample_metadata=sample_metadata,
          analysis_metadata=analysis_metadata,
          nfcore_pipeline_name=nfcore_pipeline_name)
    if nfcore_pipeline_name == 'nf-core/rnafusion':
      work_dir, runner_file = \
        prepare_nfcore_rnafusion_input(
          runner_template_file=runner_template_file,
          config_template_file=config_template_file,
          project_name=project_name,
          hpc_data_dir=hpc_data_dir,
          dbconf_file=dbconf_file,
          sample_metadata=sample_metadata,
          analysis_metadata=analysis_metadata,
          nfcore_pipeline_name=nfcore_pipeline_name)
    if nfcore_pipeline_name == 'nf-core/rnavar':
      work_dir, runner_file = \
        prepare_nfcore_rnavar_input(
          runner_template_file=runner_template_file,
          config_template_file=config_template_file,
          project_name=project_name,
          hpc_data_dir=hpc_data_dir,
          dbconf_file=dbconf_file,
          sample_metadata=sample_metadata,
          analysis_metadata=analysis_metadata,
          nfcore_pipeline_name=nfcore_pipeline_name)
    if nfcore_pipeline_name == 'nf-core/atacseq':
      work_dir, runner_file = \
        prepare_nfcore_atacseq_input(
          runner_template_file=runner_template_file,
          config_template_file=config_template_file,
          project_name=project_name,
          hpc_data_dir=hpc_data_dir,
          dbconf_file=dbconf_file,
          sample_metadata=sample_metadata,
          analysis_metadata=analysis_metadata,
          nfcore_pipeline_name=nfcore_pipeline_name)
    if nfcore_pipeline_name == 'nf-core/chipseq':
      work_dir, runner_file = \
        prepare_nfcore_chipseq_input(
          runner_template_file=runner_template_file,
          config_template_file=config_template_file,
          project_name=project_name,
          hpc_data_dir=hpc_data_dir,
          dbconf_file=dbconf_file,
          sample_metadata=sample_metadata,
          analysis_metadata=analysis_metadata,
          nfcore_pipeline_name=nfcore_pipeline_name)
    if nfcore_pipeline_name == 'nf-core/cutandrun':
      work_dir, runner_file = \
        prepare_nfcore_cutandrun_input(
          runner_template_file=runner_template_file,
          config_template_file=config_template_file,
          project_name=project_name,
          hpc_data_dir=hpc_data_dir,
          dbconf_file=dbconf_file,
          sample_metadata=sample_metadata,
          analysis_metadata=analysis_metadata,
          nfcore_pipeline_name=nfcore_pipeline_name)
    if nfcore_pipeline_name == 'nf-core/bactmap':
      work_dir, runner_file = \
        prepare_nfcore_bactmap_input(
          runner_template_file=runner_template_file,
          config_template_file=config_template_file,
          project_name=project_name,
          hpc_data_dir=hpc_data_dir,
          dbconf_file=dbconf_file,
          sample_metadata=sample_metadata,
          analysis_metadata=analysis_metadata,
          nfcore_pipeline_name=nfcore_pipeline_name)
    if nfcore_pipeline_name == 'nf-core/hic':
      work_dir, runner_file = \
        prepare_nfcore_hic_input(
          runner_template_file=runner_template_file,
          config_template_file=config_template_file,
          project_name=project_name,
          hpc_data_dir=hpc_data_dir,
          dbconf_file=dbconf_file,
          sample_metadata=sample_metadata,
          analysis_metadata=analysis_metadata,
          nfcore_pipeline_name=nfcore_pipeline_name)
    if work_dir is None or \
       runner_file is None:
      raise TypeError(
        f"Failed to get work-dir or runner-file for {nfcore_pipeline_name}")
    return work_dir, runner_file
  except Exception as e:
    raise ValueError(
      f"Error in NFCore pipeline input generation, error: {e}")


def parse_sample_metadata_and_fetch_fastq(
      sample_metadata: dict,
      dbconf_file: str) -> \
        pd.DataFrame:
  try:
    ## get list of samples from sample_metadata
    sample_igf_id_list = \
      list(sample_metadata.keys())
    if len(sample_igf_id_list) == 0:
      raise ValueError("No sample id found in sample metadata")
    ## fetch fastq files for the samples
    fastq_df = \
      get_fastq_and_run_for_samples(
        dbconfig_file=dbconf_file,
        sample_igf_id_list=sample_igf_id_list)
    if not isinstance(fastq_df, list):
      raise TypeError(
        f'Expecting a list, got {type(fastq_df)}')
    fastq_df = \
      pd.DataFrame(fastq_df)
    if 'sample_igf_id' not in fastq_df.columns or \
       'run_igf_id' not in fastq_df.columns or \
       'file_path' not in fastq_df.columns:
      raise KeyError(
        'Missing sample_igf_id or run_igf_id or file_path in fastq_df')
    return fastq_df
  except Exception as e:
    raise ValueError(
      f"Failed to get fastq for samples, error: {e}")


def format_nextflow_conf(
      config_template_file: str,
      singularity_bind_dir_list: list,
      output_dir: str,
      input_paths: str = '') -> \
        str:
  try:
    check_file_path(output_dir)
    output_file = \
      os.path.join(
        output_dir,
        os.path.basename(config_template_file))
    if len(singularity_bind_dir_list) == 0:
      singularity_bind_dir_list = ""                       ## Allow empty list
    else:
      singularity_bind_dir_list = \
        ','.join(singularity_bind_dir_list)
    _create_output_from_jinja_template(
      template_file=config_template_file,
      output_file=output_file,
      autoescape_list=['xml',],
      data={
        "DIR_LIST": singularity_bind_dir_list,
        "INPUT_PATHS": input_paths,
        "WORKDIR": output_dir
      })
    return output_file
  except Exception as e:
    raise ValueError(
      f"Failed to create config file, error: {e}")

def _check_and_set_col_order_for_nf_samplesheet(
        col_list: list,
        nf_col_list: list,
        skip_extra_cols: bool = True) \
          -> list:
  try:
    output_col_list = list()
    missing_col_list = list()
    for col_name in nf_col_list:
      if col_name in col_list:
        output_col_list.append(col_name)
      else:
        missing_col_list.append(col_name)
      if len(output_col_list) != len(col_list):
        extra_col_list = list(set(col_list).difference(set(output_col_list)))
        if not skip_extra_cols:
          output_col_list.extend(extra_col_list)
    if len(missing_col_list) > 0:
      raise ValueError(
        f"Missing required columns: {','.join(output_col_list)}")
    else:
      return output_col_list
  except Exception as e:
    raise ValueError(
      f"Failed to check col order for nf samplesheet, error: {e}")


def _make_nfcore_smrnaseq_input(
      sample_metadata: dict,
      fastq_df: pd.DataFrame,
      output_dir: str,
      output_file_path: str = 'input.csv',
      sample_id_input_column: str = 'sample_igf_id',
      fastq_file_input_column: str = 'file_path',
      sample_id_output_column: str = 'sample',
      fastq1_output_column: str = 'fastq_1',
      nf_samplesheet_header: list = ("sample", "fastq_1")) -> \
        str:
  try:
    fastq1_pattern = \
      re.compile(r'\S+_R1_001.fastq.gz')
    input_csv_file_path = \
      os.path.join(output_dir, output_file_path)
    if os.path.exists(input_csv_file_path):
      raise ValueError(
        f"File {input_csv_file_path} is already present")
    if sample_id_input_column not in fastq_df.columns or \
       fastq_file_input_column not in fastq_df.columns:
      raise KeyError(
        "Missing required columns in fastq list")
    final_csv_data = list()
    for sample_entry in sample_metadata.keys():
      fastq_files = \
        fastq_df[fastq_df[sample_id_input_column]==sample_entry][fastq_file_input_column].\
          values.tolist()
      if len(fastq_files) == 0:
        raise ValueError(
          f"No fastq file present for sample {sample_entry}")
      r1_fastq_list = [
        f for f in fastq_files
          if re.search(fastq1_pattern, f)]
      if len(r1_fastq_list) == 0:
        raise ValueError(
          f"Missing R1 fastq for sample {sample_entry}")
      for r1_fastq in r1_fastq_list:
        final_csv_data.\
          append({
            sample_id_output_column: sample_entry,
            fastq1_output_column: r1_fastq})
    final_csv_df = \
      pd.DataFrame(final_csv_data)
    ## get correct col order
    output_col_list = \
      _check_and_set_col_order_for_nf_samplesheet(
        col_list=final_csv_df.columns.tolist(),
        nf_col_list=nf_samplesheet_header)
    ## reorder df
    final_csv_df = \
      final_csv_df[output_col_list]
    final_csv_df.\
      to_csv(input_csv_file_path, index=False)
    return input_csv_file_path
  except Exception as e:
    raise ValueError(
      f"Failed to create input for nfcore_smrnaseq, error: {e}")


def prepare_nfcore_smrnaseq_input(
      runner_template_file: str,
      config_template_file: str,
      project_name: str,
      hpc_data_dir: str,
      dbconf_file: str,
      sample_metadata: dict,
      analysis_metadata: dict,
      nfcore_pipeline_name: str = 'nf-core/smrnaseq',
      nf_samplesheet_header: list = ("sample", "fastq_1"),
      exclude_nf_param_list: list = [
        '-resume',
        '-c',
        '-config',
        '--input',
        '--outdir',
        '-with-report',
        '-with-timeline',
        '-with-dag',
        '-with-tower',
        '-w',
        '-work-dir',
        '-with-notification']) \
        -> Tuple[str, str]:
  """
  :param runner_template_file
  :param config_template_file
  :param sample_metadata
             {sample1: "",
              sample2: ""}
  :param analysis_metadata
            { NXF_VER: x.y.z,
              nextflow_params:
                ["-profile singularity",
                 "-r 2.0.0",
                 "--protocol custom",
                 "--genome GRCm38",
                 "--mirtrace_species mmu",
                 "--three_prime_adapter AGATCGGAAGAGCACACGTCTGAACTCCAGTCAC"]}
  """
  try:
    ## get fastq df
    fastq_df = \
      parse_sample_metadata_and_fetch_fastq(
        sample_metadata=sample_metadata,
        dbconf_file=dbconf_file)
    ## get template and extend it and dump it as bash script
    work_dir = \
      get_temp_dir(use_ephemeral_space=True)                     ## get work dir
    project_data_dir = \
      os.path.join(
        hpc_data_dir,
        project_name)
    check_file_path(project_data_dir)                            ## get and check project data dir on hpc
    formatted_config_file = \
      format_nextflow_conf(
        config_template_file=config_template_file,
        singularity_bind_dir_list=[project_data_dir, work_dir],
        output_dir=work_dir)                                     ## get formatted config file for NextFlow run
    nextflow_version = \
      analysis_metadata.get('NXF_VER')                           ## get nextflow version from analysis design and its required
    if nextflow_version is None:
      raise KeyError("NXF_VER is missing from NextFlow analysis design")
    nextflow_params = \
      analysis_metadata.get('nextflow_params')                   ## get nextflow_params list and its optional
    nextflow_params_list = list()
    if nextflow_params is not None and \
       isinstance(nextflow_params, list):
      for i in nextflow_params:
        param_flag = i.split(' ')                                ## only check param flag if its separate by space from param value
        if param_flag not in exclude_nf_param_list:
          nextflow_params_list.\
            append(i)
    ## add required params
    ## prepare input file
    input_file = \
      _make_nfcore_smrnaseq_input(
        sample_metadata=sample_metadata,
        fastq_df=fastq_df,
        output_dir=work_dir,
        nf_samplesheet_header=nf_samplesheet_header)
    nextflow_params_list.\
      append(f'--input {input_file}')
    nextflow_params_list.\
      append('-resume')
    nextflow_params_list.\
      append(f"--outdir {os.path.join(work_dir, 'results')}")
    nextflow_params_list.\
      append(f"-work-dir {work_dir}")
    nextflow_params_list.\
      append(f"-with-report {os.path.join(work_dir, 'results', 'report.html')}")
    nextflow_params_list.\
      append(f"-with-dag {os.path.join(work_dir, 'results', 'dag.html')}")
    nextflow_params_list.\
      append(f"-with-timeline {os.path.join(work_dir, 'results', 'timeline.html')}")
    nextflow_params_list = \
      " ".join(nextflow_params_list)
    ## dump command file
    runner_file = \
      os.path.join(
        work_dir,
        os.path.basename(runner_template_file))
    _create_output_from_jinja_template(
      template_file=runner_template_file,
      output_file=runner_file,
      autoescape_list=['xml',],
      data={
        "NEXTFLOW_VERSION": nextflow_version,
        "NFCORE_PIPELINE_NAME": nfcore_pipeline_name,
        "NEXTFLOW_CONF": formatted_config_file,
        "NEXTFLOW_PARAMS": nextflow_params_list,
        "WORKDIR": work_dir
      })
    return work_dir, runner_file
  except Exception as e:
    raise ValueError(
      f"Failed to create input for NFCore smrnaseq pipeline, error: {e}")


def _make_nfcore_rnaseq_input(
      sample_metadata: dict,
      fastq_df: pd.DataFrame,
      output_dir: str,
      output_file_path: str = 'input.csv',
      sample_id_input_column: str = 'sample_igf_id',
      run_id_input_column: str = 'run_igf_id',
      fastq_file_input_column: str = 'file_path',
      output_sample_column: str = 'sample',
      output_fastq1_column: str = 'fastq_1',
      output_fastq2_column: str = 'fastq_2',
      nf_samplesheet_header: list = ("sample", "fastq_1", "fastq_2", "strandedness")) -> \
        str:
  try:
    fastq1_pattern = \
      re.compile(r'\S+_R1_001.fastq.gz')
    fastq2_pattern = \
      re.compile(r'\S+_R2_001.fastq.gz')
    input_csv_file_path = \
      os.path.join(output_dir, output_file_path)
    if os.path.exists(input_csv_file_path):
      raise ValueError(
        f"File {input_csv_file_path} is already present")
    if sample_id_input_column not in fastq_df.columns or \
       run_id_input_column not in fastq_df.columns or \
       fastq_file_input_column not in fastq_df.columns:
      raise KeyError(
        "Missing required columns in fastq list")
    final_csv_data = list()
    for sample_entry in sample_metadata.keys():
      sample_df = \
        fastq_df[fastq_df[sample_id_input_column]==sample_entry]
      if len(sample_df.index) == 0:
        raise ValueError(
          f"Missing fastqs for sample {sample_entry}")
      for run_id, r_data in sample_df.groupby(run_id_input_column):
        fastqs = \
          r_data[fastq_file_input_column].values.tolist()
        fastq1 = ''
        fastq2 = ''
        for f in fastqs:
          if re.search(fastq1_pattern, f):
            fastq1 = f
          if re.search(fastq2_pattern, f):
            fastq2 = f
        if fastq1 == "":
          raise ValueError(
            f"Missing fastq1 for sample {sample_entry}")
        row_data = {
          output_sample_column: sample_entry,
          output_fastq1_column: fastq1,
          output_fastq2_column: fastq2,
        }
        extra_metadata = \
          sample_metadata[sample_entry]
        if isinstance(extra_metadata, dict):
          row_data.\
            update(**extra_metadata)
        final_csv_data.\
          append(row_data)
    final_csv_df = \
      pd.DataFrame(final_csv_data)
    ## get correct col order
    output_col_list = \
      _check_and_set_col_order_for_nf_samplesheet(
        col_list=final_csv_df.columns.tolist(),
        nf_col_list=nf_samplesheet_header)
    ## reorder df
    final_csv_df = \
      final_csv_df[output_col_list]
    final_csv_df.\
      to_csv(input_csv_file_path, index=False)
    return input_csv_file_path
  except Exception as e:
    raise ValueError(
      f"Failed to create input for rnaseq, error: {e}")


def prepare_nfcore_rnaseq_input(
      runner_template_file: str,
      config_template_file: str,
      project_name: str,
      hpc_data_dir: str,
      dbconf_file: str,
      sample_metadata: dict,
      analysis_metadata: dict,
      nfcore_pipeline_name: str = 'nf-core/rnaseq',
      nf_samplesheet_header: list = ("sample", "fastq_1", "fastq_2", "strandedness"),
      exclude_nf_param_list: list = [
        '-resume',
        '-c',
        '-config',
        '--input',
        '--outdir',
        '-with-report',
        '-with-timeline',
        '-with-dag',
        '-with-tower',
        '-w',
        '-work-dir',
        '-with-notification']) \
        -> Tuple[str, str]:
  """
  :param runner_template_file
  :param config_template_file
  :param sample_metadata
             {sample1:
               {strandedness: unstranded},
             sample2:
               {strandedness: reverse}}
  :param analysis_metadata
            {NXF_VER: x.y.z,
             nextflow_params:
              ["-profile singularity",
               "-r 3.9",
               "--genome GRCm38",
               "--aligner star_rsem",
               "--seq_center IGF"]}
  """
  try:
    ## get fastq df
    fastq_df = \
      parse_sample_metadata_and_fetch_fastq(
        sample_metadata=sample_metadata,
        dbconf_file=dbconf_file)
    ## get template and extend it and dump it as bash script
    work_dir = \
      get_temp_dir(use_ephemeral_space=True)                     ## get work dir
    project_data_dir = \
      os.path.join(
        hpc_data_dir,
        project_name)
    check_file_path(project_data_dir)                            ## get and check project data dir on hpc
    formatted_config_file = \
      format_nextflow_conf(
        config_template_file=config_template_file,
        singularity_bind_dir_list=[project_data_dir, work_dir],
        output_dir=work_dir)                                     ## get formatted config file for NextFlow run
    nextflow_version = \
      analysis_metadata.get('NXF_VER')                           ## get nextflow version from analysis design and its required
    if nextflow_version is None:
      raise KeyError("NXF_VER is missing from NextFlow analysis design")
    nextflow_params = \
      analysis_metadata.get('nextflow_params')                   ## get nextflow_params list and its optional
    nextflow_params_list = list()
    if nextflow_params is not None and \
       isinstance(nextflow_params, list):
      for i in nextflow_params:
        param_flag = i.split(' ')                                ## only check param flag if its separate by space from param value
        if param_flag not in exclude_nf_param_list:
          nextflow_params_list.\
            append(i)
    ## add required params
    ## prepare input file
    input_file = \
      _make_nfcore_rnaseq_input(
        sample_metadata=sample_metadata,
        fastq_df=fastq_df,
        output_dir=work_dir,
        nf_samplesheet_header=nf_samplesheet_header)
    nextflow_params_list.\
      append(f'--input {input_file}')
    nextflow_params_list.\
      append(f"--outdir {os.path.join(work_dir, 'results')}")
    nextflow_params_list.\
      append(f"-work-dir {work_dir}")
    nextflow_params_list.\
      append('-resume')
    nextflow_params_list.\
      append(f"-with-report {os.path.join(work_dir, 'results', 'report.html')}")
    nextflow_params_list.\
      append(f"-with-dag {os.path.join(work_dir, 'results', 'dag.html')}")
    nextflow_params_list.\
      append(f"-with-timeline {os.path.join(work_dir, 'results', 'timeline.html')}")
    nextflow_params_list = \
      " ".join(nextflow_params_list)
    ## dump command file
    runner_file = \
      os.path.join(
        work_dir,
        os.path.basename(runner_template_file))
    _create_output_from_jinja_template(
      template_file=runner_template_file,
      output_file=runner_file,
      autoescape_list=['xml',],
      data={
        "NEXTFLOW_VERSION": nextflow_version,
        "NFCORE_PIPELINE_NAME": nfcore_pipeline_name,
        "NEXTFLOW_CONF": formatted_config_file,
        "NEXTFLOW_PARAMS": nextflow_params_list,
        "WORKDIR": work_dir
      })
    return work_dir, runner_file
  except Exception as e:
    raise ValueError(
      f"Failed to create input for NFCore smrnaseq pipeline, error: {e}")


def _make_nfcore_methylseq_input(
      sample_metadata: dict,
      fastq_df: pd.DataFrame,
      sample_id_input_column: str = 'sample_igf_id',
      run_id_input_column: str = 'run_igf_id',
      fastq_file_input_column: str = 'file_path') -> \
          list:
  try:
    fastq1_pattern = \
      re.compile(r'\S+_R1_001.fastq.gz')
    fastq2_pattern = \
      re.compile(r'\S+_R2_001.fastq.gz')
    if sample_id_input_column not in fastq_df.columns or \
       run_id_input_column not in fastq_df.columns or \
       fastq_file_input_column not in fastq_df.columns:
      raise KeyError(
        "Missing required columns in fastq list")
    input_paths_list = list()
    for sample_entry in sample_metadata.keys():
      sample_df = \
        fastq_df[fastq_df[sample_id_input_column]==sample_entry]
      if len(sample_df.index) == 0:
        raise ValueError(
          f"Missing fastqs for sample {sample_entry}")
      for run_id, r_data in sample_df.groupby(run_id_input_column):
        fastqs = \
          r_data[fastq_file_input_column].values.tolist()
        fastq1 = ''
        fastq2 = ''
        for f in fastqs:
          if re.search(fastq1_pattern, f):
            fastq1 = f
          if re.search(fastq2_pattern, f):
            fastq2 = f
        if fastq1 == "":
          raise ValueError(
            f"Missing fastq1 for sample {sample_entry}")
        fastq_list = [fastq1]
        if fastq2 != "":
          fastq_list.append(fastq2)
        input_paths_list.\
          append([sample_entry, fastq_list])
    if len(input_paths_list) == 0:
      raise ValueError("No sample input_paths found")
    return input_paths_list
  except Exception as e:
    raise ValueError(
      f"Failed to get input paths for NFcore methylseq pipeline, error: {e}")


def prepare_nfcore_methylseq_input(
      runner_template_file: str,
      config_template_file: str,
      project_name: str,
      hpc_data_dir: str,
      dbconf_file: str,
      sample_metadata: dict,
      analysis_metadata: dict,
      nfcore_pipeline_name: str = 'nf-core/methylseq',
      nf_samplesheet_header: list = ("sample", "fastq_1", "fastq_2"),
      exclude_nf_param_list: list = [
        '-resume',
        '-c',
        '-config',
        '--input',
        '--outdir',
        '-with-report',
        '-with-timeline',
        '-with-dag',
        '-with-tower',
        '-w',
        '-work-dir',
        '-with-notification']) \
        -> Tuple[str, str]:
  """
  :param runner_template_file
  :param config_template_file
  :param sample_metadata
             {sample1: "",
             sample2: ""}
  :param analysis_metadata
            {NXF_VER: x.y.z,
             nextflow_params:
              ["-profile singularity",
               "-r 2.0.0",
               "--aligner bismark",
               "--genome GRCm38"]}
  """
  try:
    ## get pipeline version
    pipeline_version = None
    nextflow_params = analysis_metadata.get('nextflow_params')
    if nextflow_params is not None and \
       isinstance(nextflow_params, list):
      for i in nextflow_params:
        if '-r' in i:
          pipeline_version = \
              i.replace('-r', '').strip()
    ## use rnaseq wrapper for >= 2.0.0
    if pipeline_version is None or \
       (pipeline_version is not None and \
        isinstance(pipeline_version, str) and \
        int(pipeline_version.split('.')[0]) >= 2):
      work_dir, runner_file = \
        prepare_nfcore_rnaseq_input(
          runner_template_file=runner_template_file,
          config_template_file=config_template_file,
          project_name=project_name,
          hpc_data_dir=hpc_data_dir,
          dbconf_file=dbconf_file,
          sample_metadata=sample_metadata,
          analysis_metadata=analysis_metadata,
          nfcore_pipeline_name=nfcore_pipeline_name,
          exclude_nf_param_list=exclude_nf_param_list,
          nf_samplesheet_header=nf_samplesheet_header)
    else:
      ## get fastq df
      fastq_df = \
        parse_sample_metadata_and_fetch_fastq(
          sample_metadata=sample_metadata,
          dbconf_file=dbconf_file)
      ## get template and extend it and dump it as bash script
      work_dir = \
        get_temp_dir(use_ephemeral_space=True)                     ## get work dir
      project_data_dir = \
        os.path.join(
          hpc_data_dir,
          project_name)
      check_file_path(project_data_dir)                            ## get and check project data dir on hpc
      ## prepare input path list, e.g., ['sampleA', ['sampleA_R1.fastq.gz', 'sampleA_R2.fast.gz']]
      input_paths = \
        _make_nfcore_methylseq_input(
          sample_metadata=sample_metadata,
          fastq_df=fastq_df)
      formatted_config_file = \
        format_nextflow_conf(
          config_template_file=config_template_file,
          singularity_bind_dir_list=[project_data_dir, work_dir],
          output_dir=work_dir,
          input_paths=input_paths)                                     ## get formatted config file for NextFlow run
      nextflow_version = \
        analysis_metadata.get('NXF_VER')                           ## get nextflow version from analysis design and its required
      if nextflow_version is None:
        raise KeyError("NXF_VER is missing from NextFlow analysis design")
      nextflow_params = \
        analysis_metadata.get('nextflow_params')                   ## get nextflow_params list and its optional
      nextflow_params_list = list()
      if nextflow_params is not None and \
         isinstance(nextflow_params, list):
        for i in nextflow_params:
          param_flag = i.split(' ')                                ## only check param flag if its separate by space from param value
          if param_flag not in exclude_nf_param_list:
            nextflow_params_list.\
              append(i)
      ## add required params
      nextflow_params_list.\
        append(f"--outdir {os.path.join(work_dir, 'results')}")
      nextflow_params_list.\
        append(f"-work-dir {work_dir}")
      nextflow_params_list.\
        append('-resume')
      nextflow_params_list.\
        append(f"-with-report {os.path.join(work_dir, 'results', 'report.html')}")
      nextflow_params_list.\
        append(f"-with-dag {os.path.join(work_dir, 'results', 'dag.html')}")
      nextflow_params_list.\
        append(f"-with-timeline {os.path.join(work_dir, 'results', 'timeline.html')}")
      nextflow_params_list = \
        " ".join(nextflow_params_list)
      ## dump command file
      runner_file = \
        os.path.join(
          work_dir,
          os.path.basename(runner_template_file))
      _create_output_from_jinja_template(
        template_file=runner_template_file,
        output_file=runner_file,
        autoescape_list=['xml',],
        data={
          "NEXTFLOW_VERSION": nextflow_version,
          "NFCORE_PIPELINE_NAME": nfcore_pipeline_name,
          "NEXTFLOW_CONF": formatted_config_file,
          "NEXTFLOW_PARAMS": nextflow_params_list,
          "WORKDIR": work_dir
        })
    return work_dir, runner_file
  except Exception as e:
    raise ValueError(
      f"Failed to create input for NFCore smrnaseq pipeline, error: {e}")


def prepare_nfcore_isoseq_input(
      runner_template_file: str,
      config_template_file: str,
      dbconf_file: str,
      sample_metadata: dict,
      analysis_metadata: dict) \
        -> Tuple[str, str]:
  """
  TO DO: This pipeline requires BAM and BAM.PBI as input
  Currently we have no example data for these file types for test run

  :param runner_template_file
  :param config_template_file
  :param sample_metadata
             {sample1: "",
              sample2: ""}
  :param analysis_metadata
            {NXF_VER: x.y.z,
             nextflow_params:
              ["-profile singularity",
               "-r 2.0.0",
               "--primers FILE_PATH"]}
  """
  try:
    pass
  except Exception as e:
    raise ValueError(
      f"Failed to create input for NFCore smrnaseq pipeline, error: {e}")


def _make_nfcore_sarek_input(
      sample_metadata: dict,
      fastq_df: pd.DataFrame,
      output_dir: str,
      output_file_path: str = 'input.csv',
      sample_id_input_column: str = 'sample_igf_id',
      run_id_input_column: str = 'run_igf_id',
      flowcell_id_input_column: str = 'flowcell_id',
      lane_id_input_column: str = 'lane_number',
      fastq_file_input_column: str = 'file_path',
      output_sample_column: str = 'sample',
      output_lane_column: str = 'lane',
      nf_samplesheet_header: list = ("patient", "sex", "status", "sample", "lane",  "fastq_1", "fastq_2"),
      output_fastq1_column: str = 'fastq_1',
      output_fastq2_column: str = 'fastq_2') -> \
        str:
  try:
    fastq1_pattern = \
      re.compile(r'\S+_R1_001.fastq.gz')
    fastq2_pattern = \
      re.compile(r'\S+_R2_001.fastq.gz')
    input_csv_file_path = \
      os.path.join(output_dir, output_file_path)
    if os.path.exists(input_csv_file_path):
      raise ValueError(
        f"File {input_csv_file_path} is already present")
    if sample_id_input_column not in fastq_df.columns or \
       run_id_input_column not in fastq_df.columns or \
       flowcell_id_input_column not in fastq_df.columns or \
       lane_id_input_column not in fastq_df.columns or \
       fastq_file_input_column not in fastq_df.columns:
      raise KeyError(
        "Missing required columns in fastq list")
    final_csv_data = list()
    for sample_entry in sample_metadata.keys():
      sample_df = \
        fastq_df[fastq_df[sample_id_input_column]==sample_entry]
      if len(sample_df.index) == 0:
        raise ValueError(
          f"Missing fastqs for sample {sample_entry}")
      for run_id, r_data in sample_df.groupby(run_id_input_column):
        fastqs = \
          r_data[fastq_file_input_column].values.tolist()
        fastq1 = ''
        fastq2 = ''
        for f in fastqs:
          if re.search(fastq1_pattern, f):
            fastq1 = f
          if re.search(fastq2_pattern, f):
            fastq2 = f
        if fastq1 == "":
          raise ValueError(
            f"Missing fastq1 for sample {sample_entry}")
        lane_ids = \
          r_data[lane_id_input_column].values.tolist()
        flowcell_ids = \
          r_data[flowcell_id_input_column].values.tolist()
        if len(lane_ids) == 0 or \
           len(flowcell_ids) == 0:
          raise ValueError(
            f"Flowcell or lane info not found for sample {sample_entry}")
        lane_entry = \
          f"{flowcell_ids[0]}_{lane_ids[0]}"
        row_data = {
          output_sample_column: sample_entry,
          output_lane_column: lane_entry,
          output_fastq1_column: fastq1,
          output_fastq2_column: fastq2,
        }
        extra_metadata = \
          sample_metadata[sample_entry]
        if isinstance(extra_metadata, dict):
          row_data.\
            update(**extra_metadata)
        final_csv_data.\
          append(row_data)
    final_csv_df = \
      pd.DataFrame(final_csv_data)
    ## get correct col order
    output_col_list = \
      _check_and_set_col_order_for_nf_samplesheet(
        col_list=final_csv_df.columns.tolist(),
        nf_col_list=nf_samplesheet_header)
    ## reorder df
    final_csv_df = \
      final_csv_df[output_col_list]
    final_csv_df.\
      to_csv(input_csv_file_path, index=False)
    return input_csv_file_path
  except Exception as e:
    raise ValueError(
      f"Failed to get input file for NFcore sarek, error: {e}")


def prepare_nfcore_sarek_input(
      runner_template_file: str,
      config_template_file: str,
      project_name: str,
      hpc_data_dir: str,
      dbconf_file: str,
      sample_metadata: dict,
      analysis_metadata: dict,
      nfcore_pipeline_name: str = 'nf-core/sarek',
      nf_samplesheet_header: list = ("patient", "sex", "status", "sample", "lane", "fastq_1", "fastq_2"),
      exclude_nf_param_list: list = [
        '-resume',
        '-c',
        '-config',
        '--input',
        '--outdir',
        '-with-report',
        '-with-timeline',
        '-with-dag',
        '-with-tower',
        '-w',
        '-work-dir',
        '-with-notification']) \
        -> Tuple[str, str]:
  """
  :param runner_template_file
  :param config_template_file
  :param sample_metadata
             {sample1:
               {patient: 1,
                sex: XX,
                status: 0},
             sample2:
               {patient: 2,
                sex: XY,
                status: 0}}
  :param analysis_metadata
            {NXF_VER: x.y.z,
             nextflow_params:
              ["-profile singularity",
               "-r 3.0.2",
               "--step mapping"
               "--genome GRCh38"]}
  """
  try:
    ## get fastq df
    fastq_df = \
      parse_sample_metadata_and_fetch_fastq(
        sample_metadata=sample_metadata,
        dbconf_file=dbconf_file)
    ## get template and extend it and dump it as bash script
    work_dir = \
      get_temp_dir(use_ephemeral_space=True)                     ## get work dir
    project_data_dir = \
      os.path.join(
        hpc_data_dir,
        project_name)
    check_file_path(project_data_dir)                            ## get and check project data dir on hpc
    formatted_config_file = \
      format_nextflow_conf(
        config_template_file=config_template_file,
        singularity_bind_dir_list=[project_data_dir, work_dir],
        output_dir=work_dir)                                     ## get formatted config file for NextFlow run
    nextflow_version = \
      analysis_metadata.get('NXF_VER')                           ## get nextflow version from analysis design and its required
    if nextflow_version is None:
      raise KeyError(
        "NXF_VER is missing from NextFlow analysis design")
    nextflow_params = \
      analysis_metadata.get('nextflow_params')                   ## get nextflow_params list and its optional
    nextflow_params_list = list()
    if nextflow_params is not None and \
       isinstance(nextflow_params, list):
      for i in nextflow_params:
        param_flag = i.split(' ')                                ## only check param flag if its separate by space from param value
        if param_flag not in exclude_nf_param_list:
          nextflow_params_list.\
            append(i)
    ## add required params
    ## prepare input file
    input_file = \
      _make_nfcore_sarek_input(
        sample_metadata=sample_metadata,
        fastq_df=fastq_df,
        output_dir=work_dir,
        nf_samplesheet_header=nf_samplesheet_header)
    nextflow_params_list.\
      append(f'--input {input_file}')
    nextflow_params_list.\
      append(f"--outdir {os.path.join(work_dir, 'results')}")
    nextflow_params_list.\
      append(f"-work-dir {work_dir}")
    nextflow_params_list.\
      append('-resume')
    nextflow_params_list.\
      append(f"-with-report {os.path.join(work_dir, 'results', 'report.html')}")
    nextflow_params_list.\
      append(f"-with-dag {os.path.join(work_dir, 'results', 'dag.html')}")
    nextflow_params_list.\
      append(f"-with-timeline {os.path.join(work_dir, 'results', 'timeline.html')}")
    nextflow_params_list = \
      " ".join(nextflow_params_list)
    ## dump command file
    runner_file = \
      os.path.join(
        work_dir,
        os.path.basename(runner_template_file))
    _create_output_from_jinja_template(
      template_file=runner_template_file,
      output_file=runner_file,
      autoescape_list=['xml',],
      data={
        "NEXTFLOW_VERSION": nextflow_version,
        "NFCORE_PIPELINE_NAME": nfcore_pipeline_name,
        "NEXTFLOW_CONF": formatted_config_file,
        "NEXTFLOW_PARAMS": nextflow_params_list,
        "WORKDIR": work_dir
      })
    return work_dir, runner_file
  except Exception as e:
    raise ValueError(
      f"Failed to create input for NFCore smrnaseq pipeline, error: {e}")


def _make_nfcore_ampliseq_input(
      sample_metadata: dict,
      fastq_df: pd.DataFrame,
      output_dir: str,
      output_file_path: str = 'input.csv',
      output_metadata_file_path: str = 'metadata.csv',
      sample_id_input_column: str = 'sample_igf_id',
      run_id_input_column: str = 'run_igf_id',
      flowcell_id_input_column: str = 'flowcell_id',
      lane_id_input_column: str = 'lane_number',
      fastq_file_input_column: str = 'file_path',
      output_sample_column: str = 'sampleID',
      output_metadata_sample_column: str = 'ID',
      output_lane_column: str = 'run',
      output_fastq1_column: str = 'forwardReads',
      output_fastq2_column: str = 'reverseReads') -> \
        Tuple[str, str]:
  try:
    fastq1_pattern = \
      re.compile(r'\S+_R1_001.fastq.gz')
    fastq2_pattern = \
      re.compile(r'\S+_R2_001.fastq.gz')
    input_csv_file_path = \
      os.path.join(output_dir, output_file_path)
    input_metadata_csv_file_path = \
      os.path.join(output_dir, output_metadata_file_path)
    if os.path.exists(input_csv_file_path):
      raise ValueError(
        f"File {input_csv_file_path} is already present")
    if sample_id_input_column not in fastq_df.columns or \
       run_id_input_column not in fastq_df.columns or \
       flowcell_id_input_column not in fastq_df.columns or \
       lane_id_input_column not in fastq_df.columns or \
       fastq_file_input_column not in fastq_df.columns:
      raise KeyError(
        "Missing required columns in fastq list")
    final_csv_data = list()
    final_metadata_csv_data = list()
    for sample_entry in sample_metadata.keys():
      sample_df = \
        fastq_df[fastq_df[sample_id_input_column]==sample_entry]
      if len(sample_df.index) == 0:
        raise ValueError(
          f"Missing fastqs for sample {sample_entry}")
      extra_metadata = \
        sample_metadata[sample_entry]
      row_metadata = {
        output_metadata_sample_column: sample_entry}
      row_metadata.\
        update(**extra_metadata)
      final_metadata_csv_data.\
        append(row_metadata)
      for run_id, r_data in sample_df.groupby(run_id_input_column):
        fastqs = \
          r_data[fastq_file_input_column].values.tolist()
        fastq1 = ''
        fastq2 = ''
        for f in fastqs:
          if re.search(fastq1_pattern, f):
            fastq1 = f
          if re.search(fastq2_pattern, f):
            fastq2 = f
        if fastq1 == "":
          raise ValueError(
            f"Missing fastq1 for sample {sample_entry}")
        lane_ids = \
          r_data[lane_id_input_column].values.tolist()
        flowcell_ids = \
          r_data[flowcell_id_input_column].values.tolist()
        if len(lane_ids) == 0 or \
           len(flowcell_ids) == 0:
          raise ValueError(
            f"Flowcell or lane info not found for sample {sample_entry}")
        lane_entry = \
          f"{flowcell_ids[0]}_{lane_ids[0]}"
        final_csv_data.append({
          output_sample_column: sample_entry,
          output_lane_column: lane_entry,
          output_fastq1_column: fastq1,
          output_fastq2_column: fastq2,
        })
    final_csv_df = \
      pd.DataFrame(final_csv_data)
    final_csv_df.\
      to_csv(input_csv_file_path, index=False)
    final_metadata_csv_df = \
      pd.DataFrame(final_metadata_csv_data)
    final_metadata_csv_df.\
      to_csv(input_metadata_csv_file_path, index=False)
    return input_csv_file_path, input_metadata_csv_file_path
  except Exception as e:
    raise ValueError(
      f"Failed to create input files for apliseq, error: {e}")


def prepare_nfcore_ampliseq_input(
      runner_template_file: str,
      config_template_file: str,
      project_name: str,
      hpc_data_dir: str,
      dbconf_file: str,
      sample_metadata: dict,
      analysis_metadata: dict,
      nfcore_pipeline_name: str = 'nf-core/ampliseq',
      exclude_nf_param_list: list = [
        '-resume',
        '-c',
        '-config',
        '--input',
        '--metadata',
        '--outdir',
        '-with-report',
        '-with-timeline',
        '-with-dag',
        '-with-tower',
        '-w',
        '-work-dir',
        '-with-notification']) -> \
          Tuple[str, str]:
  """
  :param runner_template_file
  :param config_template_file
  :param sample_metadata
             {sample1: {"condition": "control"},
              sample2: {"condition": "treatment"}}
  :param analysis_metadata
            {NXF_VER: x.y.z,
             nextflow_params:
              ["-profile singularity",
               "-r 2.4.0",
               "--illumina_novaseq",
               "--FW_primer GTGYCAGCMGCCGCGGTAA",
               "--RV_primer GGACTACNVGGGTWTCTAAT"]}
  """
  try:
    ## get fastq df
    fastq_df = \
      parse_sample_metadata_and_fetch_fastq(
        sample_metadata=sample_metadata,
        dbconf_file=dbconf_file)
    ## get template and extend it and dump it as bash script
    work_dir = \
      get_temp_dir(use_ephemeral_space=True)                     ## get work dir
    project_data_dir = \
      os.path.join(
        hpc_data_dir,
        project_name)
    check_file_path(project_data_dir)                            ## get and check project data dir on hpc
    formatted_config_file = \
      format_nextflow_conf(
        config_template_file=config_template_file,
        singularity_bind_dir_list=[project_data_dir, work_dir],
        output_dir=work_dir)                                     ## get formatted config file for NextFlow run
    nextflow_version = \
      analysis_metadata.get('NXF_VER')                           ## get nextflow version from analysis design and its required
    if nextflow_version is None:
      raise KeyError(
        "NXF_VER is missing from NextFlow analysis design")
    nextflow_params = \
      analysis_metadata.get('nextflow_params')                   ## get nextflow_params list and its optional
    nextflow_params_list = list()
    if nextflow_params is not None and \
       isinstance(nextflow_params, list):
      for i in nextflow_params:
        param_flag = i.split(' ')                                ## only check param flag if its separate by space from param value
        if param_flag not in exclude_nf_param_list:
          nextflow_params_list.\
            append(i)
    ## add required params
    ## prepare input file
    input_file, metadata_file = \
      _make_nfcore_ampliseq_input(
        sample_metadata=sample_metadata,
        fastq_df=fastq_df,
        output_dir=work_dir)
    nextflow_params_list.\
      append(f'--input {input_file}')
    nextflow_params_list.\
      append('-resume')
    nextflow_params_list.\
      append(f'--metadata {metadata_file}')
    nextflow_params_list.\
      append(f"-work-dir {work_dir}")
    nextflow_params_list.\
      append(f"--outdir {os.path.join(work_dir, 'results')}")
    nextflow_params_list.\
      append(f"-with-report {os.path.join(work_dir, 'results', 'report.html')}")
    nextflow_params_list.\
      append(f"-with-dag {os.path.join(work_dir, 'results', 'dag.html')}")
    nextflow_params_list.\
      append(f"-with-timeline {os.path.join(work_dir, 'results', 'timeline.html')}")
    nextflow_params_list = \
      " ".join(nextflow_params_list)
    ## dump command file
    runner_file = \
      os.path.join(
        work_dir,
        os.path.basename(runner_template_file))
    _create_output_from_jinja_template(
      template_file=runner_template_file,
      output_file=runner_file,
      autoescape_list=['xml',],
      data={
        "NEXTFLOW_VERSION": nextflow_version,
        "NFCORE_PIPELINE_NAME": nfcore_pipeline_name,
        "NEXTFLOW_CONF": formatted_config_file,
        "NEXTFLOW_PARAMS": nextflow_params_list,
        "WORKDIR": work_dir
      })
    return work_dir, runner_file
  except Exception as e:
    raise ValueError(
      f"Failed to create input for NFCore smrnaseq pipeline, error: {e}")

def prepare_nfcore_hic_input(
      runner_template_file: str,
      config_template_file: str,
      project_name: str,
      hpc_data_dir: str,
      dbconf_file: str,
      sample_metadata: dict,
      analysis_metadata: dict,
      nfcore_pipeline_name: str = 'nf-core/hic',
      nf_samplesheet_header: list = ("sample", "fastq_1", "fastq_2"),
      exclude_nf_param_list: list = [
        '-resume',
        '-c',
        '-config',
        '--input',
        '--outdir',
        '-with-report',
        '-with-timeline',
        '-with-dag',
        '-with-tower',
        '-w',
        '-work-dir',
        '-with-notification']) \
        -> Tuple[str, str]:
  """
  :param runner_template_file
  :param config_template_file
  :param sample_metadata
             {sample1: "",
             sample2: ""}
  :param analysis_metadata
            {NXF_VER: x.y.z,
             nextflow_base_params:
              ["-profile singularity",
               "-r 2.0.0",
               "--genome GRCh38",
               "--genomes_base path"]
  """
  try:
    work_dir, runner_file = \
      prepare_nfcore_rnaseq_input(
      runner_template_file=runner_template_file,
      config_template_file=config_template_file,
      project_name=project_name,
      hpc_data_dir=hpc_data_dir,
      dbconf_file=dbconf_file,
      sample_metadata=sample_metadata,
      analysis_metadata=analysis_metadata,
      nfcore_pipeline_name=nfcore_pipeline_name,
      exclude_nf_param_list=exclude_nf_param_list,
      nf_samplesheet_header=nf_samplesheet_header)
    return work_dir, runner_file
  except Exception as e:
    raise ValueError(
      f"Failed to create input for NFCore HiC pipeline, error: {e}")


def prepare_nfcore_rnafusion_input(
      runner_template_file: str,
      config_template_file: str,
      project_name: str,
      hpc_data_dir: str,
      dbconf_file: str,
      sample_metadata: dict,
      analysis_metadata: dict,
      nfcore_pipeline_name: str = 'nf-core/rnafusion',
      nf_samplesheet_header: list = ("sample", "fastq_1", "fastq_2", "strandedness"),
      exclude_nf_param_list: list = [
        '-resume',
        '-c',
        '-config',
        '--input',
        '--outdir',
        '-with-report',
        '-with-timeline',
        '-with-dag',
        '-with-tower',
        '-w',
        '-work-dir',
        '-with-notification']) \
        -> Tuple[str, str]:
  """
  :param runner_template_file
  :param config_template_file
  :param sample_metadata
             {sample1:
               {strandedness: unstranded},
             sample2:
               {strandedness: reverse}}
  :param analysis_metadata
            {NXF_VER: x.y.z,
             nextflow_base_params:
              ["-profile singularity",
               "-r 2.1.0",
               "--genome GRCh38",
               "--genomes_base path"]
  """
  try:
    work_dir, runner_file = \
      prepare_nfcore_rnaseq_input(
      runner_template_file=runner_template_file,
      config_template_file=config_template_file,
      project_name=project_name,
      hpc_data_dir=hpc_data_dir,
      dbconf_file=dbconf_file,
      sample_metadata=sample_metadata,
      analysis_metadata=analysis_metadata,
      nfcore_pipeline_name=nfcore_pipeline_name,
      exclude_nf_param_list=exclude_nf_param_list,
      nf_samplesheet_header=nf_samplesheet_header)
    return work_dir, runner_file
  except Exception as e:
    raise ValueError(
      f"Failed to create input for NFCore rnafusion pipeline, error: {e}")


def prepare_nfcore_rnavar_input(
      runner_template_file: str,
      config_template_file: str,
      project_name: str,
      hpc_data_dir: str,
      dbconf_file: str,
      sample_metadata: dict,
      analysis_metadata: dict,
      nfcore_pipeline_name: str = 'nf-core/rnavar',
      nf_samplesheet_header: list = ("sample", "fastq_1", "fastq_2"),
      exclude_nf_param_list: list = [
        '-resume',
        '-c',
        '-config',
        '--input',
        '--outdir',
        '-with-report',
        '-with-timeline',
        '-with-dag',
        '-with-tower',
        '-w',
        '-work-dir',
        '-with-notification']) \
        -> Tuple[str, str]:
  """
  :param runner_template_file
  :param config_template_file
  :param sample_metadata
             {sample1:
               {strandedness: unstranded},
             sample2:
               {strandedness: reverse}}
  :param analysis_metadata
            {NXF_VER: x.y.z,
             nextflow_params:
              ["-profile singularity",
               "-r 1.0.0",
               "--genome GRCh38",
               "--aligner star",
               "--seq_center IGF",
               "--seq_platform illumina"]}
  """
  try:
    work_dir, runner_file = \
      prepare_nfcore_rnaseq_input(
      runner_template_file=runner_template_file,
      config_template_file=config_template_file,
      project_name=project_name,
      hpc_data_dir=hpc_data_dir,
      dbconf_file=dbconf_file,
      sample_metadata=sample_metadata,
      analysis_metadata=analysis_metadata,
      nfcore_pipeline_name=nfcore_pipeline_name,
      exclude_nf_param_list=exclude_nf_param_list,
      nf_samplesheet_header=nf_samplesheet_header)
    return work_dir, runner_file
  except Exception as e:
    raise ValueError(
      f"Failed to create input for NFCore smrnaseq pipeline, error: {e}")


def _make_nfcore_atacseq_input(sample_metadata: dict,
      fastq_df: pd.DataFrame,
      output_dir: str,
      output_file_path: str = 'input.csv',
      sample_id_input_column: str = 'sample_igf_id',
      run_id_input_column: str = 'run_igf_id',
      fastq_file_input_column: str = 'file_path',
      output_fastq1_column: str = 'fastq_1',
      output_fastq2_column: str = 'fastq_2',
      output_group_column: str = 'sample',
      output_replicate_column: str = 'replicate',
      nf_samplesheet_header: list = ("sample", "fastq_1", "fastq_2", "replicate")) -> \
        str:
  try:
    fastq1_pattern = \
      re.compile(r'\S+_R1_001.fastq.gz')
    fastq2_pattern = \
      re.compile(r'\S+_R2_001.fastq.gz')
    input_csv_file_path = \
      os.path.join(output_dir, output_file_path)
    if os.path.exists(input_csv_file_path):
      raise ValueError(
        f"File {input_csv_file_path} is already present")
    if sample_id_input_column not in fastq_df.columns or \
       run_id_input_column not in fastq_df.columns or \
       fastq_file_input_column not in fastq_df.columns:
      raise KeyError(
        "Missing required columns in fastq list")
    final_csv_data = list()
    for sample_entry in sample_metadata.keys():
      sample_df = \
        fastq_df[fastq_df[sample_id_input_column]==sample_entry]
      if len(sample_df.index) == 0:
        raise ValueError(
          f"Missing fastqs for sample {sample_entry}")
      for run_id, r_data in sample_df.groupby(run_id_input_column):
        fastqs = \
          r_data[fastq_file_input_column].values.tolist()
        fastq1 = ''
        fastq2 = ''
        for f in fastqs:
          if re.search(fastq1_pattern, f):
            fastq1 = f
          if re.search(fastq2_pattern, f):
            fastq2 = f
        if fastq1 == "":
          raise ValueError(
            f"Missing fastq1 for sample {sample_entry}")
        row_data = {
          output_fastq1_column: fastq1,
          output_fastq2_column: fastq2,
        }
        extra_metadata = \
          sample_metadata[sample_entry]
        if isinstance(extra_metadata, dict):
          row_data.\
            update(**extra_metadata)
        final_csv_data.\
          append(row_data)
    final_csv_df = \
      pd.DataFrame(final_csv_data)
    if output_group_column not in final_csv_df.columns or \
       output_replicate_column not in final_csv_df.columns:
      raise KeyError(
        f"Missing {output_group_column} or {output_replicate_column} in sample_metadata")
    ## get correct col order
    output_col_list = \
      _check_and_set_col_order_for_nf_samplesheet(
        col_list=final_csv_df.columns.tolist(),
        nf_col_list=nf_samplesheet_header)
    ## reorder df
    final_csv_df = \
      final_csv_df[output_col_list]
    final_csv_df.\
      to_csv(input_csv_file_path, index=False)
    return input_csv_file_path
  except Exception as e:
    raise ValueError(f"Failed to get input for atacseq, error: {e}")


def prepare_nfcore_atacseq_input(
      runner_template_file: str,
      config_template_file: str,
      project_name: str,
      hpc_data_dir: str,
      dbconf_file: str,
      sample_metadata: dict,
      analysis_metadata: dict,
      nfcore_pipeline_name: str = 'nf-core/atacseq',
      nf_samplesheet_header: list = ("sample", "fastq_1", "fastq_2", "replicate"),
      exclude_nf_param_list: list = [
        '-resume',
        '-c',
        '-config',
        '--input',
        '--outdir',
        '-with-report',
        '-with-timeline',
        '-with-dag',
        '-with-tower',
        '-w',
        '-work-dir',
        '-with-notification']) \
        -> Tuple[str, str]:
  """
  :param runner_template_file
  :param config_template_file
  :param sample_metadata yaml
             {sample1:
               {group: control,
               replicate: 1},
             sample2:
               {group: control,
                replicate: 2}}
  :param analysis_metadata
            {NXF_VER: x.y.z
             nextflow_params:
              ["-profile singularity",
               "-r 1.2.2",
               "--genome GRCh38"]}
  """
  try:
    ## get fastq df
    fastq_df = \
      parse_sample_metadata_and_fetch_fastq(
        sample_metadata=sample_metadata,
        dbconf_file=dbconf_file)
    ## get template and extend it and dump it as bash script
    work_dir = \
      get_temp_dir(use_ephemeral_space=True)                     ## get work dir
    project_data_dir = \
      os.path.join(
        hpc_data_dir,
        project_name)
    check_file_path(project_data_dir)                            ## get and check project data dir on hpc
    formatted_config_file = \
      format_nextflow_conf(
        config_template_file=config_template_file,
        singularity_bind_dir_list=[project_data_dir, work_dir],
        output_dir=work_dir)                                     ## get formatted config file for NextFlow run
    nextflow_version = \
      analysis_metadata.get('NXF_VER')                           ## get nextflow version from analysis design and its required
    if nextflow_version is None:
      raise KeyError(
        "NXF_VER is missing from NextFlow analysis design")
    nextflow_params = \
      analysis_metadata.get('nextflow_params')                   ## get nextflow_params list and its optional
    nextflow_params_list = list()
    if nextflow_params is not None and \
       isinstance(nextflow_params, list):
      for i in nextflow_params:
        param_flag = i.split(' ')                                ## only check param flag if its separate by space from param value
        if param_flag not in exclude_nf_param_list:
          nextflow_params_list.\
            append(i)
    ## add required params
    ## prepare input file
    input_file = \
      _make_nfcore_atacseq_input(
        sample_metadata=sample_metadata,
        fastq_df=fastq_df,
        output_dir=work_dir,
        nf_samplesheet_header=nf_samplesheet_header)
    nextflow_params_list.\
      append(f'--input {input_file}')
    nextflow_params_list.\
      append(f"--outdir {os.path.join(work_dir, 'results')}")
    nextflow_params_list.\
      append('-resume')
    nextflow_params_list.\
      append(f"-work-dir {work_dir}")
    nextflow_params_list.\
      append(f"-with-report {os.path.join(work_dir, 'results', 'report.html')}")
    nextflow_params_list.\
      append(f"-with-dag {os.path.join(work_dir, 'results', 'dag.html')}")
    nextflow_params_list.\
      append(f"-with-timeline {os.path.join(work_dir, 'results', 'timeline.html')}")
    nextflow_params_list = \
      " ".join(nextflow_params_list)
    ## dump command file
    runner_file = \
      os.path.join(
        work_dir,
        os.path.basename(runner_template_file))
    _create_output_from_jinja_template(
      template_file=runner_template_file,
      output_file=runner_file,
      autoescape_list=['xml',],
      data={
        "NEXTFLOW_VERSION": nextflow_version,
        "NFCORE_PIPELINE_NAME": nfcore_pipeline_name,
        "NEXTFLOW_CONF": formatted_config_file,
        "NEXTFLOW_PARAMS": nextflow_params_list,
        "WORKDIR": work_dir
      })
    return work_dir, runner_file
  except Exception as e:
    raise ValueError(
      f"Failed to create input for NFCore smrnaseq pipeline, error: {e}")


def _make_nfcore_chipseq_input(sample_metadata: dict,
      fastq_df: pd.DataFrame,
      output_dir: str,
      output_file_path: str = 'input.csv',
      sample_id_input_column: str = 'sample_igf_id',
      run_id_input_column: str = 'run_igf_id',
      fastq_file_input_column: str = 'file_path',
      output_sample_column: str = 'sample',
      output_fastq1_column: str = 'fastq_1',
      output_fastq2_column: str = 'fastq_2',
      output_control_column: str = 'control',
      output_antibody_column: str = 'antibody',
      nf_samplesheet_header: list = ("sample", "fastq_1", "fastq_2", "antibody", "control")) -> \
        str:
  try:
    fastq1_pattern = \
      re.compile(r'\S+_R1_001.fastq.gz')
    fastq2_pattern = \
      re.compile(r'\S+_R2_001.fastq.gz')
    input_csv_file_path = \
      os.path.join(output_dir, output_file_path)
    if os.path.exists(input_csv_file_path):
      raise ValueError(
        f"File {input_csv_file_path} is already present")
    if sample_id_input_column not in fastq_df.columns or \
       run_id_input_column not in fastq_df.columns or \
       fastq_file_input_column not in fastq_df.columns:
      raise KeyError(
        "Missing required columns in fastq list")
    final_csv_data = list()
    for sample_entry in sample_metadata.keys():
      sample_df = \
        fastq_df[fastq_df[sample_id_input_column]==sample_entry]
      if len(sample_df.index) == 0:
        raise ValueError(
          f"Missing fastqs for sample {sample_entry}")
      for run_id, r_data in sample_df.groupby(run_id_input_column):
        fastqs = \
          r_data[fastq_file_input_column].values.tolist()
        fastq1 = ''
        fastq2 = ''
        for f in fastqs:
          if re.search(fastq1_pattern, f):
            fastq1 = f
          if re.search(fastq2_pattern, f):
            fastq2 = f
        if fastq1 == "":
          raise ValueError(
            f"Missing fastq1 for sample {sample_entry}")
        row_data = {
          output_sample_column: sample_entry,
          output_fastq1_column: fastq1,
          output_fastq2_column: fastq2,
        }
        extra_metadata = \
          sample_metadata[sample_entry]
        if isinstance(extra_metadata, dict):
          row_data.\
            update(**extra_metadata)
        final_csv_data.\
          append(row_data)
    final_csv_df = \
      pd.DataFrame(final_csv_data)
    if output_control_column not in final_csv_df.columns or \
       output_antibody_column not in final_csv_df.columns:
      raise KeyError(
        f"Missing {output_control_column} or {output_antibody_column} in sample_metadata")
    ## get correct col order
    output_col_list = \
      _check_and_set_col_order_for_nf_samplesheet(
        col_list=final_csv_df.columns.tolist(),
        nf_col_list=nf_samplesheet_header)
    ## reorder df
    final_csv_df = \
      final_csv_df[output_col_list]
    final_csv_df.\
      to_csv(input_csv_file_path, index=False)
    return input_csv_file_path
  except Exception as e:
    raise ValueError(f"Failed to get input for chipseq, error: {e}")


def prepare_nfcore_chipseq_input(
      runner_template_file: str,
      config_template_file: str,
      project_name: str,
      hpc_data_dir: str,
      dbconf_file: str,
      sample_metadata: dict,
      analysis_metadata: dict,
      nfcore_pipeline_name: str = 'nf-core/chipseq',
      nf_samplesheet_header: list = ("sample", "fastq_1", "fastq_2", "antibody", "control"),
      exclude_nf_param_list: list = [
        '-resume',
        '-c',
        '-config',
        '--input',
        '--outdir',
        '-with-report',
        '-with-timeline',
        '-with-dag',
        '-with-tower',
        '-w',
        '-work-dir',
        '-with-notification']) \
        -> Tuple[str, str]:
  """
  :param runner_template_file
  :param config_template_file
  :param sample_metadata
             {sample1:
               {antibody: BCATENIN,
                control: sample2},
             sample2:
               {antibody: "",
                control: ""}}
  :param analysis_metadata
            {NXF_VER: x.y.z,
             nextflow_params:
              ["-profile singularity",
               "-r 2.0.0",
               "--genome GRCh38"]}
  """
  try:
    ## get fastq df
    fastq_df = \
      parse_sample_metadata_and_fetch_fastq(
        sample_metadata=sample_metadata,
        dbconf_file=dbconf_file)
    ## get template and extend it and dump it as bash script
    work_dir = \
      get_temp_dir(use_ephemeral_space=True)                     ## get work dir
    project_data_dir = \
      os.path.join(
        hpc_data_dir,
        project_name)
    check_file_path(project_data_dir)                            ## get and check project data dir on hpc
    formatted_config_file = \
      format_nextflow_conf(
        config_template_file=config_template_file,
        singularity_bind_dir_list=[project_data_dir, work_dir],
        output_dir=work_dir)                                     ## get formatted config file for NextFlow run
    nextflow_version = \
      analysis_metadata.get('NXF_VER')                           ## get nextflow version from analysis design and its required
    if nextflow_version is None:
      raise KeyError(
        "NXF_VER is missing from NextFlow analysis design")
    nextflow_params = \
      analysis_metadata.get('nextflow_params')                   ## get nextflow_params list and its optional
    nextflow_params_list = list()
    if nextflow_params is not None and \
       isinstance(nextflow_params, list):
      for i in nextflow_params:
        param_flag = i.split(' ')                                ## only check param flag if its separate by space from param value
        if param_flag not in exclude_nf_param_list:
          nextflow_params_list.\
            append(i)
    ## add required params
    ## prepare input file
    input_file = \
      _make_nfcore_chipseq_input(
        sample_metadata=sample_metadata,
        fastq_df=fastq_df,
        output_dir=work_dir,
        nf_samplesheet_header=nf_samplesheet_header)
    nextflow_params_list.\
      append(f'--input {input_file}')
    nextflow_params_list.\
      append(f"--outdir {os.path.join(work_dir, 'results')}")
    nextflow_params_list.\
      append('-resume')
    nextflow_params_list.\
      append(f"-work-dir {work_dir}")
    nextflow_params_list.\
      append(f"-with-report {os.path.join(work_dir, 'results', 'report.html')}")
    nextflow_params_list.\
      append(f"-with-dag {os.path.join(work_dir, 'results', 'dag.html')}")
    nextflow_params_list.\
      append(f"-with-timeline {os.path.join(work_dir, 'results', 'timeline.html')}")
    nextflow_params_list = \
      " ".join(nextflow_params_list)
    ## dump command file
    runner_file = \
      os.path.join(
        work_dir,
        os.path.basename(runner_template_file))
    _create_output_from_jinja_template(
      template_file=runner_template_file,
      output_file=runner_file,
      autoescape_list=['xml',],
      data={
        "NEXTFLOW_VERSION": nextflow_version,
        "NFCORE_PIPELINE_NAME": nfcore_pipeline_name,
        "NEXTFLOW_CONF": formatted_config_file,
        "NEXTFLOW_PARAMS": nextflow_params_list,
        "WORKDIR": work_dir
      })
    return work_dir, runner_file
  except Exception as e:
    raise ValueError(
      f"Failed to create input for NFCore smrnaseq pipeline, error: {e}")

def _make_nfcore_cutandrun_input(sample_metadata: dict,
      fastq_df: pd.DataFrame,
      output_dir: str,
      output_file_path: str = 'input.csv',
      sample_id_input_column: str = 'sample_igf_id',
      run_id_input_column: str = 'run_igf_id',
      fastq_file_input_column: str = 'file_path',
      output_fastq1_column: str = 'fastq_1',
      output_fastq2_column: str = 'fastq_2',
      output_group_column: str = 'group',
      output_control_column: str = 'control',
      output_replicate_column: str = 'replicate',
      nf_samplesheet_header: list = ("group", "replicate", "fastq_1", "fastq_2", "control")) -> \
        str:
  try:
    fastq1_pattern = \
      re.compile(r'\S+_R1_001.fastq.gz')
    fastq2_pattern = \
      re.compile(r'\S+_R2_001.fastq.gz')
    input_csv_file_path = \
      os.path.join(output_dir, output_file_path)
    if os.path.exists(input_csv_file_path):
      raise ValueError(
        f"File {input_csv_file_path} is already present")
    if sample_id_input_column not in fastq_df.columns or \
       run_id_input_column not in fastq_df.columns or \
       fastq_file_input_column not in fastq_df.columns:
      raise KeyError(
        "Missing required columns in fastq list")
    final_csv_data = list()
    for sample_entry in sample_metadata.keys():
      sample_df = \
        fastq_df[fastq_df[sample_id_input_column]==sample_entry]
      if len(sample_df.index) == 0:
        raise ValueError(
          f"Missing fastqs for sample {sample_entry}")
      for run_id, r_data in sample_df.groupby(run_id_input_column):
        fastqs = \
          r_data[fastq_file_input_column].values.tolist()
        fastq1 = ''
        fastq2 = ''
        for f in fastqs:
          if re.search(fastq1_pattern, f):
            fastq1 = f
          if re.search(fastq2_pattern, f):
            fastq2 = f
        if fastq1 == "":
          raise ValueError(
            f"Missing fastq1 for sample {sample_entry}")
        row_data = {
          output_fastq1_column: fastq1,
          output_fastq2_column: fastq2,
        }
        extra_metadata = \
          sample_metadata[sample_entry]
        if isinstance(extra_metadata, dict):
          row_data.\
            update(**extra_metadata)
        final_csv_data.\
          append(row_data)
    final_csv_df = \
      pd.DataFrame(final_csv_data)
    if output_group_column not in final_csv_df.columns or \
       output_control_column not in final_csv_df.columns or \
       output_replicate_column not in final_csv_df.columns:
      raise KeyError(
        f"Missing {output_group_column} or {output_replicate_column} or {output_control_column} in sample_metadata")
    ## get correct col order
    output_col_list = \
      _check_and_set_col_order_for_nf_samplesheet(
        col_list=final_csv_df.columns.tolist(),
        nf_col_list=nf_samplesheet_header)
    ## reorder df
    final_csv_df = \
      final_csv_df[output_col_list]
    final_csv_df.\
      to_csv(input_csv_file_path, index=False)
    return input_csv_file_path
  except Exception as e:
    raise ValueError(f"Failed to get input for cutandrun, error: {e}")


def prepare_nfcore_cutandrun_input(
      runner_template_file: str,
      config_template_file: str,
      project_name: str,
      hpc_data_dir: str,
      dbconf_file: str,
      sample_metadata: dict,
      analysis_metadata: dict,
      nfcore_pipeline_name: str = 'nf-core/cutandrun',
      nf_samplesheet_header: list = ("group", "replicate", "fastq_1", "fastq_2", "control"),
      exclude_nf_param_list: list = [
        '-resume',
        '-c',
        '-config',
        '--input',
        '--outdir',
        '-with-report',
        '-with-timeline',
        '-with-dag',
        '-with-tower',
        '-w',
        '-work-dir',
        '-with-notification']) \
        -> Tuple[str, str]:
  """
  :param runner_template_file
  :param config_template_file
  :param sample_metadata
             {sample1:
               {group: h3k27me3,
                replicate: 1,
                control: igg_ctrl},
             sample2:
               {group: igg_ctrl,
                replicate: 1,
                control: ""}}
  :param analysis_metadata
            {NXF_VER: x.y.z,
             nextflow_params:
              ["-profile singularity",
               "-r 3.0",
               "--genome GRCh38"]}
  """
  try:
    ## get fastq df
    fastq_df = \
      parse_sample_metadata_and_fetch_fastq(
        sample_metadata=sample_metadata,
        dbconf_file=dbconf_file)
    ## get template and extend it and dump it as bash script
    work_dir = \
      get_temp_dir(use_ephemeral_space=True)                     ## get work dir
    project_data_dir = \
      os.path.join(
        hpc_data_dir,
        project_name)
    check_file_path(project_data_dir)                            ## get and check project data dir on hpc
    formatted_config_file = \
      format_nextflow_conf(
        config_template_file=config_template_file,
        singularity_bind_dir_list=[project_data_dir, work_dir],
        output_dir=work_dir)                                     ## get formatted config file for NextFlow run
    nextflow_version = \
      analysis_metadata.get('NXF_VER')                           ## get nextflow version from analysis design and its required
    if nextflow_version is None:
      raise KeyError(
        "NXF_VER is missing from NextFlow analysis design")
    nextflow_params = \
      analysis_metadata.get('nextflow_params')                   ## get nextflow_params list and its optional
    nextflow_params_list = list()
    if nextflow_params is not None and \
       isinstance(nextflow_params, list):
      for i in nextflow_params:
        param_flag = i.split(' ')                                ## only check param flag if its separate by space from param value
        if param_flag not in exclude_nf_param_list:
          nextflow_params_list.\
            append(i)
    ## add required params
    ## prepare input file
    input_file = \
      _make_nfcore_cutandrun_input(
        sample_metadata=sample_metadata,
        fastq_df=fastq_df,
        output_dir=work_dir,
        nf_samplesheet_header=nf_samplesheet_header)
    nextflow_params_list.\
      append(f'--input {input_file}')
    nextflow_params_list.\
      append(f"--outdir {os.path.join(work_dir, 'results')}")
    nextflow_params_list.\
      append('-resume')
    nextflow_params_list.\
      append(f"-work-dir {work_dir}")
    nextflow_params_list.\
      append(f"-with-report {os.path.join(work_dir, 'results', 'report.html')}")
    nextflow_params_list.\
      append(f"-with-dag {os.path.join(work_dir, 'results', 'dag.html')}")
    nextflow_params_list.\
      append(f"-with-timeline {os.path.join(work_dir, 'results', 'timeline.html')}")
    nextflow_params_list = \
      " ".join(nextflow_params_list)
    ## dump command file
    runner_file = \
      os.path.join(
        work_dir,
        os.path.basename(runner_template_file))
    _create_output_from_jinja_template(
      template_file=runner_template_file,
      output_file=runner_file,
      autoescape_list=['xml',],
      data={
        "NEXTFLOW_VERSION": nextflow_version,
        "NFCORE_PIPELINE_NAME": nfcore_pipeline_name,
        "NEXTFLOW_CONF": formatted_config_file,
        "NEXTFLOW_PARAMS": nextflow_params_list,
        "WORKDIR": work_dir
      })
    return work_dir, runner_file
  except Exception as e:
    raise ValueError(
      f"Failed to create input for NFCore cutandrun pipeline, error: {e}")


def prepare_nfcore_bactmap_input(
      runner_template_file: str,
      config_template_file: str,
      project_name: str,
      hpc_data_dir: str,
      dbconf_file: str,
      sample_metadata: dict,
      analysis_metadata: dict,
      nfcore_pipeline_name: str = 'nf-core/bactmap',
      nf_samplesheet_header: list = ('sample', 'fastq_1', 'fastq_2'),
      exclude_nf_param_list: list = [
        '-resume',
        '-c',
        '-config',
        '--input',
        '--outdir',
        '-with-report',
        '-with-timeline',
        '-with-dag',
        '-with-tower',
        '-w',
        '-work-dir',
        '-with-notification']) \
        -> Tuple[str, str]:
  """
  :param runner_template_file
  :param config_template_file
  :param sample_metadata
             {sample1: "",
              sample2: ""}
  :param analysis_metadata
            {NXF_VER: x.y.z,
             nextflow_params:
              ["-profile singularity",
               "-r 1.0.0",
               "--reference path"]}
  """
  try:
    work_dir, runner_file = \
      prepare_nfcore_smrnaseq_input(
        runner_template_file=runner_template_file,
        config_template_file=config_template_file,
        project_name=project_name,
        hpc_data_dir=hpc_data_dir,
        dbconf_file=dbconf_file,
        sample_metadata=sample_metadata,
        analysis_metadata=analysis_metadata,
        nfcore_pipeline_name=nfcore_pipeline_name,
        exclude_nf_param_list=exclude_nf_param_list,
        nf_samplesheet_header=nf_samplesheet_header)
    return work_dir, runner_file
  except Exception as e:
    raise ValueError(
      f"Failed to create input for NFCore smrnaseq pipeline, error: {e}")