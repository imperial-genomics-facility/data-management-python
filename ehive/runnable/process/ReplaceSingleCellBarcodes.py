
from ehive.runnable.IGFBaseProcess import IGFBaseProcess

class ReplaceSingleCellBarcodes(IGFBaseProcess):
  '''
  A class for replacing 10X single cell barcodes present on the samplesheet
  It checks the Description column of the samplesheet and look for the specific
  single_cell_lebel. Also it requires a json format file listing all the single
  cell barcodes downloaded from this page
  https://support.10xgenomics.com/single-cell-gene-expression/sequencing/doc/
  specifications-sample-index-sets-for-single-cell-3
  '''
  def param_defaults(self):
    params_dict=super(CheckAndProcessSampleSheet,self).param_defaults()
    return params_dict
  
  def run(self):
    try:
      igf_session_class = self.param_required('igf_session_class')
    except:
      message='seqrun: {2}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise