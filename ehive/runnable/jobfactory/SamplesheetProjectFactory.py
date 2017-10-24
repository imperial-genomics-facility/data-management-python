#!/usr/bin/env python
import os
from ehive.runnable.IGFBaseJobFactory import IGFBaseJobFactory
from igf_data.illumina.samplesheet import SampleSheet

class SampleSheetProjectFactory(IGFBaseJobFactory):
  '''
  A class for finding all the projects mentioned in the SampleSheet
  '''
  def param_defaults(self):
    params_dict=super(IGFBaseJobFactory,self).param_defaults()
    params_dict.update({
        '10X_label':'10X'
      })
    return params_dict
  
  
  def run(self):
    try:
      samplesheet_file=self.param_required('samplesheet')
      seqrun_igf_id=self.param_required('seqrun_igf_id')
      tenX_label=self.param('10X_label')
      
      if not os.path.exists(samplesheet_file):
        raise IOError('Samplesheet file {0} not found'.format(samplesheet_file))
      
      samplesheet=SampleSheet(infile=samplesheet_file)                          # read samplesheet
      samplesheet.filter_sample_data(condition_key='Description', 
                                     condition_value=tenX_label, 
                                     method='exclude')                          # separate 10X samplesheet
      project_names=samplesheet.get_project_names()                             # get project names from samplesheet
      sub_tasks=[{'project_name':project_name} \
                 for project_name in project_names]                             # create subtasks data structure
      self.param('sub_tasks',sub_tasks)                                         # seed dataflow
      self.add_asana_notes(task_name=seqrun_igf_id,\
                           notes='\n'.join(project_names))                      # add project names to asana notes
    except Exception as e:
      message='seqrun: {2}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise