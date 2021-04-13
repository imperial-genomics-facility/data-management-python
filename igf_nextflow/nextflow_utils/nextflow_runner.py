import os,subprocess
from igf_data.utils.fileutils import check_file_path
from igf_data.utils.fileutils import get_temp_dir
from igf_nextflow.nextflow_utils.nextflow_config_formatter import format_nextflow_config_file
from igf_nextflow.nextflow_utils.nextflow_design import extend_nextflow_analysis_design_and_params

def nextflow_pre_run_setup(
      nextflow_exe,analysis_description,dbconf_file,nextflow_config_template,
      igenomes_base_path,igenome_base_path=None,hpc_queue_name='pqcgi',
      use_ephemeral_space=True):
  try:
    extended_analysis_design,extended_analysis_params,input_dir_list = \
      extend_nextflow_analysis_design_and_params(
        analysis_description=analysis_description,
        dbconf_file=dbconf_file)
    work_dir = \
      get_temp_dir(use_ephemeral_space=use_ephemeral_space)
    output_nextflow_config = \
      os.path.join(work_dir,'nextflow.cfg')
    if igenome_base_path is not None:
      input_dir_list.append(igenome_base_path)                                  # add refgenome dir to singularity bind dir list
    format_nextflow_config_file(
      template_file=nextflow_config_template,
      output_file=output_nextflow_config,
      hpc_queue_name=hpc_queue_name,
      bind_dir_list=input_dir_list)
    extended_analysis_params.\
      append('-c output_nextflow_config')                                       # adding nextflow config to params
    nextflow_pipeline = analysis_description.get('nextflow_pipeline')
    if nextflow_pipeline is None:
      raise KeyError('Missing the value for nextflow_pipeline key')
    if nextflow_pipeline == 'atacseq':
      pass
    elif nextflow_pipeline == 'chipseq':
      pass
    elif nextflow_pipeline == 'sarek':
      raise NotImplementedError()
    else:
      raise ValueError('Pipeline {0} not supported'.format(nextflow_pipeline))
  except Exception as e:
    raise ValueError(
            'Failed to run nextflow pipeline, error: {0}'.format(e))

