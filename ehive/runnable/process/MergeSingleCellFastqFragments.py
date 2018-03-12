from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.process.singlecell_seqrun.mergesinglecellfastq import MergeSingleCellFastq
class MergeSingleCellFastqFragments(IGFBaseProcess):
  '''
  An ehive runnable class for merging single cell fastq fragments after 
  BCL2FASTQ runs
  '''
  def param_defaults(self):
    params_dict=super(CheckAndProcessSampleSheet,self).param_defaults()
    params_dict.update({
                        'singlecell_tag':'10X'
                      })
    return params_dict

  def run(self):
    try:
      samplesheet_file = self.param_required('samplesheet')
      fastq_dir=self.param_required('fastq_dir')
      seqrun_igf_id=self.param_required('seqrun_igf_id')
      model_name=self.param_required('model_name')
      singlecell_tag=self.param_required('singlecell_tag')
      merge_fastq=MergeSingleCellFastq(fastq_dir=fastq_dir,
                                       samplesheet=samplesheet_file,
                                       platform_name=model_name,
                                       singlecell_tag=singlecell_tag)           # merge fastq instance
      merge_fastq.merge_fastq_per_lane_per_sample()                             # merge fastq fragments for singlecell samples
      self.param('dataflow_params',{'fastq_dir':fastq_dir})
    except Exception as e:
      message='seqrun: {2}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise