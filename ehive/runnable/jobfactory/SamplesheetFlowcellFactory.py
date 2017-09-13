#!/usr/bin/env python
import os
from ehive.runnable.IGFBaseJobFactory import IGFBaseJobFactory
from igf_data.illumina.samplesheet import SampleSheet

class SampleSheetFlowcellFactory(IGFBaseJobFactory):
  '''
  A class for finding all the projects mentioned in the SampleSheet
  '''
  def param_defaults(self):
    params_dict=IGFBaseProcess.param_defaults()
    params_dict.update({
        'samplesheet':None
      })
    return params_dict
  
  def run(self):
    try:
      samplesheet_file=self.param_required('samplesheet')
      project_name=self.param_required('project_name')
      if not os.path.exists(samplesheet_file):
        raise IOError('Samplesheet file {0} not found'.format(samplesheet_file))
      
      samplesheet=SampleSheet(infile=samplesheet_file)                          # read samplesheet
      samplesheet.filter_sample_data(condition_key='Sample_Project', 
                                     condition_value=project_name, 
                                     method='include')                          # keep only selected project
      flowcell_lanes=samplesheet.get_lane_count()                               # get flowcell lanes for the selected projects
      sub_tasks=[{'project_name':project_name,'flowcell_lane':lane} \
                 for lane in flowcell_lanes]                                    # create data structure for sub_tasks
      self.param('sub_tasks',sub_tasks)                                         # seed dataflow  
    except:
      raise