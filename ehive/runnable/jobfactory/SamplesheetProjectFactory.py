#!/usr/bin/env python
import os
from ehive.runnable.IGFBaseJobFactory import IGFBaseJobFactory
from igf_data.illumina.samplesheet import SampleSheet

class SampleSheetProjectFactory(IGFBaseJobFactory):
  '''
  A class for finding all the projects mentioned in the SampleSheet
  '''
  def param_defaults(self):
    params_dict=IGFBaseProcess.param_defaults()
    params_dict.update({
        'samplesheet':None,
        '10X_label':'10X'
      })
    return params_dict
  
  
  def run(self):
    try:
      samplesheet_file=self.param_required('samplesheet')
      if not os.path.exists(samplesheet_file):
        raise IOError('Samplesheet file {0} not found'.format(samplesheet_file))
      
      samplesheet=SampleSheet(infile=samplesheet_file)                          # read samplesheet
      samplesheet.filter_sample_data(condition_key='Description', 
                                     condition_value=tenX_label, 
                                     method='exclude')                          # separate 10X samplesheet
      project_names=samplesheet.get_project_names()                             # get project names from samplesheet
      sub_tasks=[{'project_name':project_name}for project_name in project_names]# create subtasks data structure
      self.param('sub_tasks',sub_tasks)                                         # seed dataflow
    except:
      raise