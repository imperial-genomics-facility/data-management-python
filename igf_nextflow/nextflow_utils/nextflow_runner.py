import os,subprocess
import pandas as pd
from igf_data.utils.fileutils import check_file_path
from igf_data.utils.fileutils import get_temp_dir
from igf_nextflow.nextflow_utils.nextflow_config_formatter import format_nextflow_config_file
from igf_nextflow.nextflow_utils.nextflow_design import extend_nextflow_analysis_design_and_params

def nextflow_pre_run_setup(
      nextflow_exe,analysis_description,dbconf_file,nextflow_config_template,
      igenomes_base_path=None,hpc_queue_name='pqcgi',use_ephemeral_space=True,
      nextflow_singularity_cache_dir=None):
  '''
  A function to prepare Nextflow run execution

  :param nextflow_exe:
  :param analysis_description: A dictionary containing the following keys
    * nextflow_pipeline
    * nextflow_design
    * nextflow_params
    * use_local_igenomes_base
  :param dbconf_file: DB config file
  :param nextflow_config_template: Nextflow config template file
  :param igenomes_base_path: igenomes local path, default None
  :param hpc_queue_name: HPC queue name specific for user, default pqcgi
  :param use_ephemeral_space: A toggle for using ephemeral space, default True
  :param nextflow_singularity_cache_dir: Nextflow singularity cache dir, default None
  :returns:
    * A list of Nextflow command and params
    * A string containing the work dir path
  '''
  try:
    extended_analysis_design,extended_analysis_params,input_dir_list = \
      extend_nextflow_analysis_design_and_params(
        analysis_description=analysis_description,
        dbconf_file=dbconf_file,
        igenomes_base_path=igenomes_base_path)
    if not isinstance(extended_analysis_params,list) or \
       len(extended_analysis_params)==0:
      raise ValueError('No param list found for Nextflow run')
    check_file_path(nextflow_exe)
    extended_analysis_params.\
      insert(0,nextflow_exe)
    work_dir = \
      get_temp_dir(use_ephemeral_space=use_ephemeral_space)
    input_dir_list.append(work_dir)
    output_nextflow_config = \
      os.path.join(work_dir,'nextflow.cfg')
    if igenomes_base_path is not None:
      input_dir_list.append(igenomes_base_path)                                 # add refgenome dir to singularity bind dir list
    format_nextflow_config_file(
      template_file=nextflow_config_template,
      output_file=output_nextflow_config,
      hpc_queue_name=hpc_queue_name,
      bind_dir_list=input_dir_list,
      nextflow_singularity_cache_dir=nextflow_singularity_cache_dir)                                             # get formatted config file
    extended_analysis_params.\
      append('-c {0}'.format(output_nextflow_config))                           # adding nextflow config to params
    nextflow_pipeline = \
      analysis_description.get('nextflow_pipeline')
    if nextflow_pipeline is None:
      raise KeyError('Missing the value for nextflow_pipeline key')
    if nextflow_pipeline == 'atacseq':
      analysis_design_file = \
        os.path.join(work_dir,'atacseq_input_design.csv')
      extended_analysis_design = \
        pd.DataFrame(extended_analysis_design)
      extended_analysis_design.\
        to_csv(analysis_design_file,index=False)                                # writing atacseq input design
      extended_analysis_params.\
        append('--input {0}'.format(analysis_design_file))
    elif nextflow_pipeline == 'chipseq':
      analysis_design_file = \
        os.path.join(work_dir,'chipseq_input_design.csv')
      extended_analysis_design = \
        pd.DataFrame(extended_analysis_design)
      extended_analysis_design.\
        to_csv(analysis_design_file,index=False)
      extended_analysis_params.\
        append('--input {0}'.format(analysis_design_file))
    elif nextflow_pipeline == 'sarek':
      analysis_design_file = \
        os.path.join(work_dir,'sarek_input_design.tsv')
      extended_analysis_design = \
        pd.DataFrame(extended_analysis_design)
      extended_analysis_design.\
        to_csv(analysis_design_file,index=False,header=False)                   # no header for sarek pipeline
      extended_analysis_params.\
        append('--input {0}'.format(analysis_design_file))
      raise NotImplementedError()
    else:
      raise ValueError(
              'Pipeline {0} not supported'.format(nextflow_pipeline))
    return extended_analysis_params,work_dir
  except Exception as e:
    raise ValueError(
            'Failed to run nextflow pipeline, error: {0}'.format(e))