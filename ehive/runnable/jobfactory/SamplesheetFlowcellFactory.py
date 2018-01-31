#!/usr/bin/env python
import os
from ehive.runnable.IGFBaseJobFactory import IGFBaseJobFactory
from igf_data.illumina.samplesheet import SampleSheet

class SampleSheetFlowcellFactory(IGFBaseJobFactory):
  '''
  A class for finding all the projects mentioned in the SampleSheet
  '''
  def param_defaults(self):
    params_dict=super(SampleSheetFlowcellFactory,self).param_defaults()
    return params_dict
  
  def run(self):
    try:
      samplesheet_file=self.param_required('samplesheet')
      project_name=self.param_required('project_name')
      seqrun_igf_id=self.param_required('seqrun_igf_id')
      
      if not os.path.exists(samplesheet_file):
        raise IOError('Samplesheet file {0} not found'.format(samplesheet_file))
      
      samplesheet=SampleSheet(infile=samplesheet_file)                          # read samplesheet
      samplesheet.filter_sample_data(condition_key='Sample_Project', 
                                     condition_value=project_name, 
                                     method='include')                          # keep only selected project
      flowcell_lanes=samplesheet.get_lane_count()                               # get flowcell lanes for the selected projects
      if not len(flowcell_lanes)>0:
        raise ValueError('project {0} is not present in the samplesheet {1}'.\
                         format(project_name,seqrun_igf_id))
      
      sub_tasks=[{'project_name':project_name,'flowcell_lane':lane} \
                 for lane in flowcell_lanes]                                    # create data structure for sub_tasks
      self.param('sub_tasks',sub_tasks)                                         # seed dataflow
      message='seqrun: {0}, project {1}, lanes: {1}'.\
              format(seqrun_igf_id,\
                     project_name,\
                     ','.join(flowcell_lanes))
      self.post_message_to_slack(message,reaction='pass')                       # send log to slack
      self.comment_asana_task(task_name=seqrun_igf_id, comment=message)         # send log to asana
    except Exception as e:
      message='seqrun: {2}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise