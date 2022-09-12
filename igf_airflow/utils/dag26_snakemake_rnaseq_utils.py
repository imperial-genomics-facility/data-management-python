import os
import re
import yaml
from typing import Tuple
import pandas as pd
from igf_data.utils.analysis_fastq_fetch_utils import get_fastq_and_run_for_samples
from yaml import Loader, Dumper
from typing import Tuple, Union
from igf_data.utils.fileutils import check_file_path
from igf_data.utils.fileutils import get_temp_dir



def parse_design_and_build_inputs_for_snakemake_rnaseq(
      input_design_yaml: str,
      dbconfig_file: str,
      work_dir: str,
      config_yaml_filename: str = 'config.yaml',
      units_tsv_filename: str = 'units.tsv',
      samples_tsv_filename: str = 'samples.tsv') \
        -> Tuple[str, str, str]:
  try:
    check_file_path(input_design_yaml)
    check_file_path(dbconfig_file)
    check_file_path(work_dir)
    sample_metadata, analysis_metadata = \
      parse_analysus_design_and_get_metadata(
        input_design_yaml=input_design_yaml)
    if sample_metadata is None or \
       analysis_metadata is None:
        raise KeyError("Missing sample or analysis metadata")
    ## get sample ids from metadata
    sample_igf_id_list = \
      list(sample_metadata.keys())
    if len(sample_igf_id_list) == 0:
      raise ValueError("No sample id found in the metadata")
    ## get fastq files for all samples
    fastq_list = \
      get_fastq_and_run_for_samples(
        dbconfig_file=dbconfig_file,
        sample_igf_id_list=sample_igf_id_list)
    if len(fastq_list) == 0:
      raise ValueError(
        f"No fastq file found for samples: {input_design_yaml}")
    ## get data for sample and units tsv file
    samples_tsv_list, unites_tsv_list = \
      prepare_sample_and_units_tsv_for_snakemake_rnaseq(
        sample_metadata=sample_metadata,
        fastq_list=fastq_list)
    ## get work dir and dump snakemake input files
    if len(samples_tsv_list) == 0:
      raise ValueError("Missing samples tsv data")
    if len(unites_tsv_list) == 0:
      raise ValueError("Missing units tsv data")
    units_tsv_file = \
      os.path.join(
        work_dir,
        units_tsv_filename)
    pd.DataFrame(unites_tsv_list).\
      to_csv(
        units_tsv_file,
        sep="\t",
        index=False)
    samples_tsv_file = \
      os.path.join(
        work_dir,
        samples_tsv_filename)
    pd.DataFrame(samples_tsv_list).\
      to_csv(
        samples_tsv_file,
        sep="\t",
        index=False)
    ## dump config yaml file
    config_yaml_file = \
      os.path.join(
        work_dir,
        config_yaml_filename)
    config_yaml = dict()
    config_yaml.\
      update({
        'samples': samples_tsv_file,
        'units': units_tsv_file})
    config_yaml.\
      update(**analysis_metadata)
    output_yaml = \
      yaml.dump(
        config_yaml,
        Dumper=Dumper,
        sort_keys=False)
    with open(config_yaml_file, 'w') as fp:
      fp.write(output_yaml)
    return config_yaml_file, samples_tsv_file, units_tsv_file
  except Exception as e:
    raise ValueError(
      f"Failed to parse analysis design and generate snakemake input, error: {e}")


def parse_analysus_design_and_get_metadata(
      input_design_yaml: str,
      sample_metadata_key: str = 'sample_metadata',
      analysis_metadata_key: str = 'analysis_metadata') \
      -> Tuple[Union[dict, None], Union[dict, None]]:
  try:
    check_file_path(input_design_yaml)
    with open(input_design_yaml, 'r') as fp:
      yaml_data = yaml.load(fp, Loader=Loader)
    sample_metadata = \
      yaml_data.get(sample_metadata_key)
    analysis_metadata = \
      yaml_data.get(analysis_metadata_key)
    return sample_metadata, analysis_metadata
  except Exception as e:
    raise ValueError(
      f"Failed to parse analysis design, error: {e}")


def prepare_sample_and_units_tsv_for_snakemake_rnaseq(
      sample_metadata: dict,
      fastq_list: list,
      sample_igf_id_key : str = 'sample_igf_id',
      file_path_key: str = 'file_path',
      fastq_group_columns: list = ['flowcell_id', 'lane_number'],
      units_tsv_columns: list = ['sra', 'adapters', 'strandedness']) \
        -> Tuple[list, list]:
  try:
    unites_tsv_list = list()
    samples_tsv_list = list()
    fq1_pattern = \
      re.compile(r'\S+_R1_001.fastq.gz')
    fq2_pattern = \
      re.compile(r'\S+_R2_001.fastq.gz')
    fastq_df = pd.DataFrame(fastq_list)
    for sample_name, sample_info in sample_metadata.items():
      samples_tsv_row = dict()
      samples_tsv_row.update({
        'sample_name': sample_name})
      ## add additional columns to samples_tsv
      for key, val in sample_info.items():
        if key not in units_tsv_columns:
          samples_tsv_row.\
            update({key: val})
      samples_tsv_list.\
        append(samples_tsv_row)
      sample_fastq = \
        fastq_df[fastq_df[sample_igf_id_key] == sample_name]
      if len(sample_fastq.index) == 0:
        raise ValueError(
          f"No fastq entry found for {sample_name}")
      check_fastq_columns = [
        f for f in sample_fastq.columns
          if f in fastq_group_columns]
      if len(check_fastq_columns) != len(fastq_group_columns):
        raise KeyError(
          f"Missing required keys in fastq list: {fastq_group_columns}")
      for (flowcell_id, lane_number), u_data in sample_fastq.groupby(fastq_group_columns):
        ## default paths are empty string
        fq1 = ''
        fq2 = ''
        ## assign fastqs to units_tsv
        for f in u_data[file_path_key].values.tolist():
          if re.match(fq1_pattern, f):
            fq1 = f
          if re.match(fq2_pattern, f):
            fq2 = f
        unites_tsv_row = dict()
        unites_tsv_row.update({
          'sample_name': sample_name,
          'unit_name': f'{flowcell_id}_{lane_number}',
          'fq1': fq1,
          'fq2': fq2})
        ## add additional columns to units_tsv
        for col_name in units_tsv_columns:
          if col_name in sample_info:
            unites_tsv_row.\
              update({
                col_name: sample_info.get(col_name)})
          else:
            unites_tsv_row.\
              update({
                col_name: ''})
        unites_tsv_list.\
          append(unites_tsv_row)
    return samples_tsv_list, unites_tsv_list
  except Exception as e:
    raise ValueError(
      f"Failed to parse analysis design and generate snakemake input, error: {e}")