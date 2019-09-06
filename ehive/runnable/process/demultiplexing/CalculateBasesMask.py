#!/usr/bun/env python
import os
from igf_data.illumina.basesMask import BasesMask
from ehive.runnable.IGFBaseProcess import IGFBaseProcess



class CalculateBasesMask(IGFBaseProcess):
  '''
  A process classs for calculation of bases mask value based on
  Illumina samplesheet and RunInfor.xml file
  '''
  def param_defaults(self):
    params_dict=super(CalculateBasesMask,self).param_defaults()
    params_dict.update({
        'runinfo_filename':'RunInfo.xml',
        'read_offset':1,
        'index_offset':0,
        'custom_bases_mask':None
      })
    return params_dict
  
  
  def run(self):
    try:
      seqrun_igf_id = self.param_required('seqrun_igf_id')
      seqrun_local_dir = self.param_required('seqrun_local_dir')
      samplesheet_file = self.param_required('samplesheet')
      flowcell_lane = self.param_required('flowcell_lane')
      project_name = self.param_required('project_name')
      runinfo_filename = self.param('runinfo_filename')
      read_offset = self.param('read_offset')
      index_offset = self.param('index_offset')
      custom_bases_mask = self.param('custom_bases_mask')

      runinfo_file = \
        os.path.join(\
          seqrun_local_dir,
          seqrun_igf_id,
          runinfo_filename)

      if not os.path.exists(samplesheet_file):
        raise IOError('samplesheet file {0} not found'.\
                      format(samplesheet_file))

      if not os.path.exists(runinfo_file):
        raise IOError('RunInfo file {0} file not found'.\
                      format(runinfo_file))

      if custom_bases_mask and \
         (custom_bases_mask is not None or \
          custom_bases_mask != ''):
        bases_mask_value = custom_bases_mask                                    # using custom bases mask
      else:
        bases_mask_object = \
          BasesMask(\
            samplesheet_file=samplesheet_file,
            runinfo_file=runinfo_file,
            read_offset=read_offset,
            index_offset=index_offset)                                          # create bases mask object
        bases_mask_value = \
          bases_mask_object.calculate_bases_mask()                              # calculate bases mask

      self.param('dataflow_params',{'basesmask':bases_mask_value})
      message = \
        'seqrun: {0}, project: {1}, lane: {2}, mask: {3}'.\
        format(seqrun_igf_id, project_name, flowcell_lane, bases_mask_value)
      self.post_message_to_slack(message,reaction='pass')                       # send log to slack
      self.comment_asana_task(task_name=seqrun_igf_id, comment=message)         # send log to asana
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