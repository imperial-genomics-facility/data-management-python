import os
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.process.singlecell_seqrun.processsinglecellsamplesheet import ProcessSingleCellSamplesheet

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
    params_dict.update({
                        'output_samplesheet_name':'SampleSheet_SC.csv',
                        'singlecell_tag':'10X',
                      })
    return params_dict


  def run(self):
    try:
      igf_session_class = self.param_required('igf_session_class')
      samplesheet_file = self.param_required('samplesheet')
      singlecell_barcode_json = self. param_required('singlecell_barcode_json')
      base_work_dir=self.param_required('base_work_dir')
      output_samplesheet_name=self.param('output_samplesheet_name')
      singlecell_tag=self.param('singlecell_tag')
      job_name=self.job_name()
      work_dir=os.path.join(base_work_dir,seqrun_igf_id,job_name)               # get work directory name
      sc_data=ProcessSingleCellSamplesheet(samplesheet_file,\
                                           singlecell_barcode_json,\
                                           singlecell_tag)
      output_samplesheet=os.path.join(work_dir,output_samplesheet_name)         # set output file
      if os.path.exists(output_samplesheet):
        os.remove(output_samplesheet)                                           # remove existing file

      sc_data.change_singlecell_barcodes(output_samplesheet)                    # print new samplesheet with sc indexes
      self.param('dataflow_params',{'samplesheet':output_samplesheet})          # set data flow
    except:
      message='seqrun: {2}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise