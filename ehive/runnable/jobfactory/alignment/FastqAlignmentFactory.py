import pandas as pd
from igf_data.utils.fastq_utils import identify_fastq_pair
from ehive.runnable.IGFBaseJobFactory import IGFBaseJobFactory

class FastqAlignmentFactory(IGFBaseJobFactory):
  '''
  A IGF jobfactory runnable for fetching all active runs for an experiment
  '''
  def param_defaults(self):
    params_dict=super(FastqAlignmentFactory,self).param_defaults()
    return params_dict

  def run(self):
    try:
      project_igf_id=self.param_required('project_igf_id')
      experiment_igf_id=self.param_required('experiment_igf_id')
      sample_igf_id=self.param_required('sample_igf_id')
      read1_list=self.param_required('read1_list')
      read2_list=self.param_required('read2_list')
      if not isinstance(read1_list,list) or \
         len(read1_list) == 0:
        raise ValueError('No R1 reads found')

      if isinstance(read2_list,list) and \
         len(read2_list) != 0 and \
         len(read1_list) != len(read2_list):
        raise ValueError('Number of R1 reads not matching with R2 reads: {0} {1}'.\
                         format(read1_list,read2_list))

      combined_file_list=list()
      combined_file_list.extend(read1_list)
      if len(read2_list) > 0:
        combined_file_list.extend(read2_list)

      sorted_r1_list,sorted_r2_list=\
          identify_fastq_pair(input_list=combined_file_list)                    # separate r1 and r2 files in sorted order
      if len(sorted_r2_list)>0:
        combined_read_df=pd.DataFrame({'r1_read_file':sorted_r1_list,
                                       'r2_read_file':sorted_r2_list,
                                      })
      else:
        combined_read_df=pd.DataFrame({'r1_read_file':sorted_r1_list})

      fastq_reads_list=combined_read_df.to_dict(orient='records')
      self.param('sub_tasks',fastq_reads_list)                                  # pass on fastq factory output list
    except Exception as e:
      message='project: {2}, sample:{3}, Error in {0}: {1}'.\
              format(self.__class__.__name__,
                     e,
                     project_igf_id,
                     sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise