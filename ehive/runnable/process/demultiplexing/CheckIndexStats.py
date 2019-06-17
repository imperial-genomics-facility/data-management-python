#!/usr/bin/env python
import os
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.fileutils import get_temp_dir
from igf_data.process.data_qc.check_sequence_index_barcodes import CheckSequenceIndexBarcodes,IndexBarcodeValidationError

class CheckIndexStats(IGFBaseProcess):
  '''
  A ehive process class for checking barcode stats and report to slack and asana
  '''
  def param_defaults(self):
    params_dict=super(CheckIndexStats,self).param_defaults()
    params_dict.update({
      'stats_filename':'Stats/Stats.json',
      'strict_check':True,
      })
    return params_dict

  def run(self):
    try:
      samplesheet_file = self.param_required('original_samplesheet')
      seqrun_igf_id = self.param_required('seqrun_igf_id')
      fastq_dir = self.param_required('fastq_dir')
      model_name = self.param_required('model_name')
      project_name = self.param_required('project_name')
      stats_filename = self.param('stats_filename')
      strict_check = self.param('strict_check')

      work_dir = \
        get_temp_dir(use_ephemeral_space=False)                                 # get work directory name
      stats_json_file = \
        os.path.join(\
          fastq_dir,
          stats_filename)                                                       # get stats file path
      barcode_stat = \
        CheckSequenceIndexBarcodes(\
          stats_json_file=stats_json_file,
          samplesheet_file=samplesheet_file,
          platform_name=model_name)                                             # create check instance
      barcode_stat.\
        validate_barcode_stats(\
          work_dir=work_dir, \
          strict_check=strict_check)                                            # validate seqrun stats
      self.param('dataflow_params',
                 {'barcode_qc_stats':'PASS'})                                   # seed dataflow parame for the qc passed lanes
    except IndexBarcodeValidationError as e:
      self.param('dataflow_params',
                 {'barcode_qc_stats':'FAIL'})                                   # seed dataflow for failed lanes
      message = \
        'project: {0}, message:{1}'.\
        format(project_name,e.message)
      if len(e.plots)==0:
        self.post_message_to_slack(\
          message=e.message,
          reaction='fail')                                                      # only post msg to slack if no plots
        self.comment_asana_task(\
          task_name=seqrun_igf_id,
          comment=e.message)                                                    # log to asana task
      else:
        for plot_file in e.plots:
          self.post_file_to_slack(message=message,filepath=plot_file)           # posting plot files to slack
          self.upload_file_to_asana_task(\
            task_name=seqrun_igf_id,
            filepath=plot_file, \
            comment=message)                                                    # upload plots to asana
    except Exception as e:
      message = \
        'seqrun: {2}, Error in {0}: {1}'.\
          format(\
            self.__class__.__name__,
            e,
            seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise