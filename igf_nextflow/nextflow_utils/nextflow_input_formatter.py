import os
import re
import pandas as pd
from typing import Tuple
from igf_data.utils.fileutils import check_file_path
from igf_data.utils.fileutils import get_temp_dir
from igf_data.utils.analysis_fastq_fetch_utils import get_fastq_and_run_for_samples
from igf_airflow.utils.dag22_bclconvert_demult_utils import _create_output_from_jinja_template


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
        "INPUT_PATHS": input_paths
      })
    return output_file
  except Exception as e:
    raise ValueError(
      f"Failed to create config file, error: {e}")


def _make_nfcore_smrnaseq_input(
      sample_metadata: dict,
      fastq_df: pd.DataFrame,
      output_dir: str,
      output_file_path: str = 'input.csv',
      sample_id_input_column: str = 'sample_igf_id',
      fastq_file_input_column: str = 'file_path',
      sample_id_output_column: str = 'sample',
      fastq1_output_column: str = 'fastq_1') -> \
        str:
  try:
    fastq1_pattern = \
      re.compile('\S+_R1_001.fastq.gz')
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
      exclude_nf_param_list: list = [
        '-resume',
        '-profile',
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
        output_dir=work_dir)
    nextflow_params_list.\
      append(f'--input {input_file}')
    nextflow_params_list.\
      append(f"--outdir {work_dir}")
    nextflow_params_list.\
      append(f"-with-report {os.path.join(work_dir, 'report.html')}")
    nextflow_params_list.\
      append(f"-with-dag {os.path.join(work_dir, 'dag.html')}")
    nextflow_params_list.\
      append(f"-with-timeline {os.path.join(work_dir, 'timeline.html')}")
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
        "NEXTFLOW_PARAMS": nextflow_params_list
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
      output_fastq2_column: str = 'fastq_2') -> \
        str:
  try:
    fastq1_pattern = \
      re.compile('\S+_R1_001.fastq.gz')
    fastq2_pattern = \
      re.compile('\S+_R2_001.fastq.gz')
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
      exclude_nf_param_list: list = [
        '-resume',
        '-profile',
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
        output_dir=work_dir)
    nextflow_params_list.\
      append(f'--input {input_file}')
    nextflow_params_list.\
      append(f"--outdir {work_dir}")
    nextflow_params_list.\
      append(f"-with-report {os.path.join(work_dir, 'report.html')}")
    nextflow_params_list.\
      append(f"-with-dag {os.path.join(work_dir, 'dag.html')}")
    nextflow_params_list.\
      append(f"-with-timeline {os.path.join(work_dir, 'timeline.html')}")
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
        "NEXTFLOW_PARAMS": nextflow_params_list
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
      re.compile('\S+_R1_001.fastq.gz')
    fastq2_pattern = \
      re.compile('\S+_R2_001.fastq.gz')
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
      exclude_nf_param_list: list = [
        '-resume',
        '-profile',
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
      append(f"--outdir {work_dir}")
    nextflow_params_list.\
      append(f"-with-report {os.path.join(work_dir, 'report.html')}")
    nextflow_params_list.\
      append(f"-with-dag {os.path.join(work_dir, 'dag.html')}")
    nextflow_params_list.\
      append(f"-with-timeline {os.path.join(work_dir, 'timeline.html')}")
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
        "NEXTFLOW_PARAMS": nextflow_params_list
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
      output_fastq1_column: str = 'fastq_1',
      output_fastq2_column: str = 'fastq_2') -> \
        str:
  try:
    fastq1_pattern = \
      re.compile('\S+_R1_001.fastq.gz')
    fastq2_pattern = \
      re.compile('\S+_R2_001.fastq.gz')
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
      exclude_nf_param_list: list = [
        '-resume',
        '-profile',
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
        output_dir=work_dir)
    nextflow_params_list.\
      append(f'--input {input_file}')
    nextflow_params_list.\
      append(f"--outdir {work_dir}")
    nextflow_params_list.\
      append(f"-with-report {os.path.join(work_dir, 'report.html')}")
    nextflow_params_list.\
      append(f"-with-dag {os.path.join(work_dir, 'dag.html')}")
    nextflow_params_list.\
      append(f"-with-timeline {os.path.join(work_dir, 'timeline.html')}")
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
        "NEXTFLOW_PARAMS": nextflow_params_list
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
      re.compile('\S+_R1_001.fastq.gz')
    fastq2_pattern = \
      re.compile('\S+_R2_001.fastq.gz')
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
        '-profile',
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
      append(f'--metadata {metadata_file}')
    nextflow_params_list.\
      append(f"--outdir {work_dir}")
    nextflow_params_list.\
      append(f"-with-report {os.path.join(work_dir, 'report.html')}")
    nextflow_params_list.\
      append(f"-with-dag {os.path.join(work_dir, 'dag.html')}")
    nextflow_params_list.\
      append(f"-with-timeline {os.path.join(work_dir, 'timeline.html')}")
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
        "NEXTFLOW_PARAMS": nextflow_params_list
      })
    return work_dir, runner_file
  except Exception as e:
    raise ValueError(
      f"Failed to create input for NFCore smrnaseq pipeline, error: {e}")


def prepare_nfcore_rnafusion_input(
      runner_template_file: str,
      config_template_file: str,
      project_name: str,
      hpc_data_dir: str,
      dbconf_file: str,
      sample_metadata: dict,
      analysis_metadata: dict,
      nfcore_pipeline_name: str = 'nf-core/rnafusion',
      exclude_nf_param_list: list = [
        '-resume',
        '-profile',
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
      exclude_nf_param_list=exclude_nf_param_list)
    return work_dir, runner_file
  except Exception as e:
    raise ValueError(
      f"Failed to create input for NFCore smrnaseq pipeline, error: {e}")


def prepare_nfcore_rnavar_input(
      runner_template_file: str,
      config_template_file: str,
      project_name: str,
      hpc_data_dir: str,
      dbconf_file: str,
      sample_metadata: dict,
      analysis_metadata: dict,
      nfcore_pipeline_name: str = 'nf-core/rnavar',
      exclude_nf_param_list: list = [
        '-resume',
        '-profile',
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
      exclude_nf_param_list=exclude_nf_param_list)
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
      output_group_column: str = 'group',
      output_replicate_column: str = 'replicate') -> \
        str:
  try:
    fastq1_pattern = \
      re.compile('\S+_R1_001.fastq.gz')
    fastq2_pattern = \
      re.compile('\S+_R2_001.fastq.gz')
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
      exclude_nf_param_list: list = [
        '-resume',
        '-profile',
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
        output_dir=work_dir)
    nextflow_params_list.\
      append(f'--input {input_file}')
    nextflow_params_list.\
      append(f"--outdir {work_dir}")
    nextflow_params_list.\
      append(f"-with-report {os.path.join(work_dir, 'report.html')}")
    nextflow_params_list.\
      append(f"-with-dag {os.path.join(work_dir, 'dag.html')}")
    nextflow_params_list.\
      append(f"-with-timeline {os.path.join(work_dir, 'timeline.html')}")
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
        "NEXTFLOW_PARAMS": nextflow_params_list
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
      output_antibody_column: str = 'antibody') -> \
        str:
  try:
    fastq1_pattern = \
      re.compile('\S+_R1_001.fastq.gz')
    fastq2_pattern = \
      re.compile('\S+_R2_001.fastq.gz')
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
      exclude_nf_param_list: list = [
        '-resume',
        '-profile',
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
        output_dir=work_dir)
    nextflow_params_list.\
      append(f'--input {input_file}')
    nextflow_params_list.\
      append(f"--outdir {work_dir}")
    nextflow_params_list.\
      append(f"-with-report {os.path.join(work_dir, 'report.html')}")
    nextflow_params_list.\
      append(f"-with-dag {os.path.join(work_dir, 'dag.html')}")
    nextflow_params_list.\
      append(f"-with-timeline {os.path.join(work_dir, 'timeline.html')}")
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
        "NEXTFLOW_PARAMS": nextflow_params_list
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
      output_replicate_column: str = 'replicate') -> \
        str:
  try:
    fastq1_pattern = \
      re.compile('\S+_R1_001.fastq.gz')
    fastq2_pattern = \
      re.compile('\S+_R2_001.fastq.gz')
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
      nfcore_pipeline_name: str = 'nf-core/atacseq',
      exclude_nf_param_list: list = [
        '-resume',
        '-profile',
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
        output_dir=work_dir)
    nextflow_params_list.\
      append(f'--input {input_file}')
    nextflow_params_list.\
      append(f"--outdir {work_dir}")
    nextflow_params_list.\
      append(f"-with-report {os.path.join(work_dir, 'report.html')}")
    nextflow_params_list.\
      append(f"-with-dag {os.path.join(work_dir, 'dag.html')}")
    nextflow_params_list.\
      append(f"-with-timeline {os.path.join(work_dir, 'timeline.html')}")
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
        "NEXTFLOW_PARAMS": nextflow_params_list
      })
    return work_dir, runner_file
  except Exception as e:
    raise ValueError(
      f"Failed to create input for NFCore cutandrun pipeline, error: {e}")


def prepare_nfcore_bactmap_input(
      runner_template_file: str,
      config_template_file: str,
      dbconf_file: str,
      sample_metadata: dict,
      analysis_metadata: dict) \
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
    pass
  except Exception as e:
    raise ValueError(
      f"Failed to create input for NFCore smrnaseq pipeline, error: {e}")