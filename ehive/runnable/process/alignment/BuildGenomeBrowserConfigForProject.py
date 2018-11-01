import os
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.config_genome_browser import Config_genome_browser

class BuildGenomeBrowserConfigForProject(IGFBaseProcess):
  def param_defaults(self):
    params_dict=super(BuildGenomeBrowserConfigForProject,self).param_defaults()
    params_dict.update({
      'ref_genome_type':'GENOME_TWOBIT_URI',
      'collection_table':'experiment'
    })
    return params_dict

  def run(self):
    '''
    '''
    try:
      project_igf_id=self.param_required('project_igf_id')
      experiment_igf_id=self.param_required('experiment_igf_id')
      sample_igf_id=self.param_required('sample_igf_id')
      igf_session_class=self.param_required('igf_session_class')
      collection_type_list=self.param_required('collection_type_list')
      ref_genome_type=self.param('ref_genome_type')
      collection_table=self.param('collection_table')
      pipeline_name=self.param_required('pipeline_name')
      species_name=self.param_required('species_name')
      base_work_dir=self.param_required('base_work_dir')
      template_file=self.param_required('template_file')
      work_dir_prefix=os.path.join(base_work_dir,
                                   project_igf_id,
                                   sample_igf_id,
                                   experiment_igf_id)
      work_dir=self.get_job_work_dir(work_dir=work_dir_prefix)                  # get a run work dir
      output_file=os.path.join(work_dir,
                               os.path.basename(template_file))                 # get output file name
      cg=Config_genome_browser(\
            dbsession_class=igf_session_class,
            project_igf_id=project_igf_id,
            collection_type_list=collection_type_list,
            pipeline_name=pipeline_name,
            collection_table=collection_table,
            species_name=species_name,
            ref_genome_type=ref_genome_type)
      cg.build_biodalliance_config(\
            template_file=template_file,
            output_file=output_file)
      if os.path.exists(output_file):
        self.param('dataflow_params',{'genome_browser_config':output_file})     # populate dataflow if the output file found
      else:
        self.param('dataflow_params',{'genome_browser_config':''})              # send empty string to dataflow

      message='Greated genome browser config for {0}: {1}'.\
              format(project_igf_id,
                     sample_igf_id)
      self.post_message_to_slack(message,reaction='pass')                       # send log to slack

    except Exception as e:
      message='project: {2}, sample:{3}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      project_igf_id,
                                                      sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise