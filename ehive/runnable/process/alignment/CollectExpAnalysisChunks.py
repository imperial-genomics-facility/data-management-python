import os
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.fileutils import get_datestamp_label

class CollectExpAnalysisChunks(IGFBaseProcess):
  def param_defaults(self):
    params_dict=super(CollectExpAnalysisChunks,self).param_defaults()
    params_dict.update({
      'output_mode':'list'
    })
    return params_dict

  def run(self):
    '''
    '''
    try:
      project_igf_id=self.param_required('project_igf_id')
      experiment_igf_id=self.param_required('experiment_igf_id')
      sample_igf_id=self.param_required('sample_igf_id')
      accu_data=self.param_required('accu_data')
      output_mode=self.param_required('output_mode')
      base_work_dir=self.param_required('base_work_dir')
      seed_date_stamp=self.param_required('date_stamp')
      seed_date_stamp=get_datestamp_label(seed_date_stamp)
      exp_analysis_files=accu_data.get(experiment_igf_id).get(seed_date_stamp)
      if exp_analysis_files is None:
        raise ValueError('No data found in accu table for exp {0} and date_stamp {1}'.\
                         format(experiment_igf_id,seed_date_stamp))             # incorrect data structure

      if isinstance(exp_analysis_files,list) and \
         len(exp_analysis_files)==0:
        raise ValueError('No run level file found in accu data for exp {0} and date_stamp {1}'.\
                         format(experiment_igf_id,seed_date_stamp))             # zero input file

      if output_mode=='list':
        self.param('dataflow_params',{'exp_chunk_list':exp_analysis_files})
      elif output_mode=='file':
        work_dir_prefix=os.path.join(base_work_dir,
                                     project_igf_id,
                                     sample_igf_id,
                                     experiment_igf_id)
        work_dir=self.get_job_work_dir(work_dir=work_dir_prefix)                # get a run work dir
        output_file=os.path.join(work_dir,'exp_level_chunk.txt')
        with open(output_file,'w') as fp:
          fp.write('\n'.join(exp_analysis_files))

        self.param('dataflow_params',{'exp_chunk_list_file':output_file})
      else:
        raise ValueError('Output mode {0} not supported'.format(output_mode))

    except Exception as e:
      message='project: {2}, sample:{3}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      project_igf_id,
                                                      sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise