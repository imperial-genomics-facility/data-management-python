import os
import pandas as pd
from igf_data.utils.analysis_fastq_fetch_utils import get_fastq_and_run_for_samples

def extend_nextflow_analysis_design_and_params(analysis_description,dbconf_file):
  '''
  A function for extending the analysis design for Nextflow pipeline run

  :param analysis_description: A dictionary containing the description from analysis table
  :param dbconf_file: DB config files for sample lookup
  :returns: Three lists for analysis_design,analysis_params and input dirs
  '''
  try:
    if not isinstance(analysis_description,dict):
      raise TypeError('Expecting a dictionary, got {0}'.format(type(analysis_description)))
    nextflow_pipeline = analysis_description.get('nextflow_pipeline')
    nextflow_design = analysis_description.get('nextflow_design')
    nextflow_params = analysis_description.get('nextflow_params')
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
    else:
      raise ValueError('analysis not supported: {0}'.format(nextflow_pipeline))
    if len(extended_analysis_params)==0:
      raise ValueError('No analysis parameter found')
    if len(extended_analysis_design)==0:
      raise ValueError('Failed to get analysis design')
    if '-profile singularity' not in extended_analysis_params:
      extended_analysis_params.append('-profile singularity')
    return extended_analysis_design,extended_analysis_params,input_dir_list
  except:
    raise

def get_nextflow_atacseq_design_and_params(analysis_description,dbconf_file):
  try:
    return extended_analysis_design,extended_analysis_params,input_dir_list
  except:
    raise