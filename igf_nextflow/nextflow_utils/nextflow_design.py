import os, re
import pandas as pd
from copy import copy
from igf_data.utils.analysis_fastq_fetch_utils import get_fastq_and_run_for_samples

def extend_nextflow_analysis_design_and_params(
      analysis_description,dbconf_file,use_singularity=True,
      igenomes_base_path=None):
  '''
  A function for extending the analysis design for Nextflow pipeline run

  :param analysis_description: A dictionary containing the description from analysis table
    analysis_description : {
      nextflow_pipeline: pipeline_name,
      nextflow_design: A list or list of dictionary containing the nextflow design
      nextflow_params: A list of nextflow params
      use_local_igenomes_base: true or None
    }
  :param dbconf_file: DB config files for sample lookup
  :param use_singularity: A toggle for using singularity profile, default True
  :param igenomes_base_path: Custom igenomes base path, default None
  :returns: Three lists for analysis_design,analysis_params and input dirs
  '''
  try:
    if not isinstance(analysis_description,dict):
      raise TypeError('Expecting a dictionary, got {0}'.format(type(analysis_description)))
    nextflow_pipeline = analysis_description.get('nextflow_pipeline')
    nextflow_design = analysis_description.get('nextflow_design')
    nextflow_params = analysis_description.get('nextflow_params')
    use_local_igenomes_base = analysis_description.get('use_local_igenomes_base')
    if nextflow_design is None or \
       nextflow_params is None:
      raise KeyError('Missing required key nextflow_params or nextflow_design')
    if not isinstance(nextflow_design,list) or \
       not isinstance(nextflow_params,list):
      raise TypeError('Expecting a list for analysis design and params')
    if len(nextflow_design)==0:
      raise ValueError('No analysis design found')
    extended_analysis_design = list()
    extended_analysis_params = list()
    input_dir_list = list()
    if nextflow_pipeline=='atacseq':
      extended_analysis_design,extended_analysis_params,input_dir_list = \
        get_nextflow_atacseq_design_and_params(analysis_description,dbconf_file)
    elif nextflow_pipeline=='chipseq':
      extended_analysis_design,extended_analysis_params,input_dir_list = \
        get_nextflow_chipseq_design_and_params(analysis_description,dbconf_file)
    elif nextflow_pipeline=='sarek':
      extended_analysis_design,extended_analysis_params,input_dir_list = \
        get_nextflow_sarek_design_and_params(analysis_description,dbconf_file)
    else:
      raise ValueError('analysis not supported: {0}'.format(nextflow_pipeline))
    if len(extended_analysis_params)==0:
      raise ValueError('No analysis parameter found')
    if len(extended_analysis_design)==0:
      raise ValueError('Failed to get analysis design')
    if '-profile singularity' not in extended_analysis_params and \
       use_singularity:
      extended_analysis_params.append('-profile singularity')
    extended_analysis_params.\
      append('-resume')
    if use_local_igenomes_base is not None:
      extended_analysis_params.\
        append('--igenomes_base {0}'.format(igenomes_base_path))
    return extended_analysis_design,extended_analysis_params,input_dir_list
  except Exception as e:
    raise ValueError(
            'Failed to get nextflow design, error: {0}'.\
              format(e))


def collect_fastq_with_run_and_pair_info_for_sample(sample_igf_id_list,dbconf_file):
  try:
    sample_fastq_data = list()
    fastq_df = \
      get_fastq_and_run_for_samples(
        dbconfig_file=dbconf_file,
        sample_igf_id_list=sample_igf_id_list)
    if not isinstance(fastq_df,list):
      raise TypeError(
              'Expecting a list, got {0}'.\
                format(type(fastq_df)))
    fastq_df = pd.DataFrame(fastq_df)
    if 'sample_igf_id' not in fastq_df.columns or \
       'run_igf_id' not in fastq_df.columns or \
       'file_path' not in fastq_df.columns:
      raise KeyError('Missing sample_igf_id or run_igf_id or file_path in fastq_df')
    for sample_igf_id,s_data in fastq_df.groupby('sample_igf_id'):
      for run_igf_id,r_data in s_data.groupby('run_igf_id'):
        r1_file = None
        r2_file = None
        r1_file_name_pattern = \
          re.compile(r'(\S+)_S\d+_L00\d_R1_001\.fastq\.gz')
        r2_file_name_pattern = \
          re.compile(r'(\S+)_S\d+_L00\d_R2_001\.fastq\.gz')
        if len(r_data.index)==1:
          r1_file =  r_data['file_path'].values[0]
          sample_fastq_data.append({
            'sample_igf_id':sample_igf_id,
            'run_igf_id':run_igf_id,
            'r1_fastq_file':r1_file})
        elif len(r_data.index)==2:
          for f in list(r_data['file_path'].values):
            if re.match(r1_file_name_pattern,f):
              r1_file = f
            if re.match(r2_file_name_pattern,f):
              r2_file = f
          if r1_file is None or \
             r2_file is None:
            raise ValueError(
                    'Fastq file not found for sample {0}: {1}'.\
                      format(sample_igf_id,r_data.to_dict(orient='records')))
          sample_fastq_data.append({
            'sample_igf_id':sample_igf_id,
            'run_igf_id':run_igf_id,
            'r1_fastq_file':r1_file,
            'r2_fastq_file':r2_file})
        else:
          raise ValueError(
                  'Incorrect number of fastq files found for sample {0}: {1}'.\
                    format(sample_igf_id,r_data.to_dict(orient='records')))
    return sample_fastq_data
  except Exception as e:
    raise ValueError(
            'Failed to get fastq for sample: {0}, error: {1}'.\
              format(sample_igf_id_list,e))


def get_nextflow_atacseq_design_and_params(
      analysis_description,dbconf_file,pipeline_name='nf-core/atacseq',
      igf_seq_center='Imperial BRC Genomics Facility'):
  try:
    extended_analysis_design = list()
    extended_analysis_params = [pipeline_name]
    input_dir_list = list()
    sample_igf_id_list = list()
    paired_read = False
    nextflow_design = \
      analysis_description.get('nextflow_design')
    nextflow_params = \
      analysis_description.get('nextflow_params')
    for entry in nextflow_design:
      for key,val in entry.items():
        if key=='sample_igf_id':
          sample_igf_id_list.\
            append(val)
    sample_fastq_data = \
      collect_fastq_with_run_and_pair_info_for_sample(
        sample_igf_id_list=sample_igf_id_list,
        dbconf_file=dbconf_file)
    sample_fastq_df = pd.DataFrame(sample_fastq_data)
    sample_fastq_df.fillna('',inplace=True)
    for entry in nextflow_design:
      sample_igf_id = entry.get('sample_igf_id')                                        # specific for atac-seq design
      if sample_igf_id is None:
        raise ValueError('Missing sample id')
      sample_fastqs = \
        sample_fastq_df[sample_fastq_df['sample_igf_id']==sample_igf_id].\
          to_dict(orient='records')
      for run in sample_fastqs:
        run_data = copy(entry)
        run_data.pop('sample_igf_id')
        fastq_1 = run.get('r1_fastq_file')
        input_dir_list.\
          append(os.path.dirname(fastq_1))
        run_data.update({'fastq_1':fastq_1})
        if 'r2_fastq_file' in run and \
           run.get('r2_fastq_file')!='':
          fastq_2 = run.get('r2_fastq_file')
          run_data.update({'fastq_2':fastq_2})
          input_dir_list.\
            append(os.path.dirname(fastq_2))                                    # allowing fastq_2 from separate temp dir
          if not paired_read:
            paired_read = True
        extended_analysis_design.append(run_data)                               # add fastq file details to design
    input_dir_list = list(set(input_dir_list))                                  # creating unique listy
    if not paired_read and \
       '--single_end' not in nextflow_params:
      extended_analysis_params.\
        append('--single_end')
    seq_center_exists = False
    for i in nextflow_params:
      if i.startswith('--seq_center'):
        seq_center_exists = True
        extended_analysis_params.\
          append('--seq_center {0}'.format(igf_seq_center))                     # change seq center name
      else:
        extended_analysis_params.append(i)
    if not seq_center_exists:
      extended_analysis_params.\
          append('--seq_center {0}'.format(igf_seq_center))                     # add seqcenter name
    return extended_analysis_design,extended_analysis_params,input_dir_list
  except Exception as e:
    raise ValueError(
            'Failed to get design and params for atac-seq, error: {0}'.\
              format(e))


def get_nextflow_chipseq_design_and_params(
      analysis_description,dbconf_file,pipeline_name='nf-core/chipseq',
      igf_seq_center='Imperial BRC Genomics Facility'):
  try:
    extended_analysis_design = list()
    extended_analysis_params = list()
    input_dir_list = list()
    extended_analysis_design,extended_analysis_params,input_dir_list = \
      get_nextflow_atacseq_design_and_params(
        analysis_description=analysis_description,
        dbconf_file=dbconf_file,
        pipeline_name=pipeline_name,
        igf_seq_center=igf_seq_center)                                          # its almost similar to atacseq conf
    return extended_analysis_design,extended_analysis_params,input_dir_list
  except Exception as e:
    raise ValueError(
            'Failed to get design and params for chip-seq, error: {0}'.\
              format(e))


def get_nextflow_sarek_design_and_params(
      analysis_description,dbconf_file,pipeline_name='nf-core/sarek',
      igf_seq_center='Imperial BRC Genomics Facility'):
  try:
    extended_analysis_design = list()
    extended_analysis_params = list()
    input_dir_list = list()
    raise NotImplementedError('Its not implemented yet')
    return extended_analysis_design,extended_analysis_params,input_dir_list
  except Exception as e:
    raise ValueError(
            'Failed to get design and params for sarek, error: {0}'.\
              format(e))