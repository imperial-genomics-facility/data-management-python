import os,json
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.tools.subread_utils import run_featureCounts
from igf_data.utils.fileutils import get_datestamp_label
from igf_data.utils.tools.reference_genome_utils import Reference_genome_utils

class RunFeatureCounts(IGFBaseProcess):
  def param_defaults(self):
    params_dict=super(RunFeatureCounts,self).param_defaults()
    params_dict.update({
        'reference_gtf':'GENE_GTF',
        'run_thread':1,
        'parameter_options':None
      })
    return params_dict

  def run(self):
    '''
    A method for running featureCounts tool
    
    '''
    try:
      project_igf_id=self.param_required('project_igf_id')
      experiment_igf_id=self.param_required('experiment_igf_id')
      sample_igf_id=self.param_required('sample_igf_id')
      featurecounts_exe=self.param_required('featurecounts_exe')
      input_files=self.param_required('input_files')
      reference_gtf=self.param('reference_gtf')
      base_work_dir=self.param_required('base_work_dir')
      igf_session_class=self.param_required('igf_session_class')
      species_name=self.param_required('species_name')
      parameter_options=self.param('parameter_options')
      run_thread=self.param('run_thread')
      output_prefix=self.param_required('output_prefix')
      seed_date_stamp=self.param_required('date_stamp')
      seed_date_stamp=get_datestamp_label(seed_date_stamp)
      work_dir_prefix=os.path.join(base_work_dir,
                                   project_igf_id,
                                   sample_igf_id,
                                   experiment_igf_id)
      work_dir=self.get_job_work_dir(work_dir=work_dir_prefix)                  # get a run work dir
      output_prefix='{0}_{1}'.format(output_prefix,
                                     seed_date_stamp)
      output_file=os.path.join(work_dir,
                               output_prefix)
      ref_genome=Reference_genome_utils(\
                   genome_tag=species_name,
                   dbsession_class=igf_session_class,
                   gene_gtf_type=reference_gtf
                 )                                                              # setup ref genome utils
      gene_gtf=ref_genome.get_gene_gtf()                                        # get gtf file
      summary_file,featureCount_cmd=\
        run_featureCounts(featurecounts_exe=featurecounts_exe,
                          input_gtf=gene_gtf,
                          input_bams=input_files,
                          output_file=output_file,
                          thread=run_thread,
                          options=parameter_options)
      self.param('dataflow_params',
                     {'featureCounts_output':output_file,
                      'featureCounts_summary':summary_file
                     })
      message='finished featureCounts for {0} {1}'.\
              format(project_igf_id,
                     run_igf_id)
      self.post_message_to_slack(message,reaction='pass')                       # send log to slack
      message='featureCounts {0} command: {1}'.\
              format(run_igf_id,
                     featureCount_cmd)
      self.comment_asana_task(task_name=project_igf_id,
                              comment=message)                                  # send commandline to Asana
    except Exception as e:
      message='project: {2}, sample:{3}, Error in {0}: {1}'.\
              format(self.__class__.__name__,
                     e,
                     project_igf_id,
                     sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise