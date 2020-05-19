#!/usr/bin/env python
import os
from ehive.runnable.IGFBaseJobFactory import IGFBaseJobFactory
from igf_data.illumina.samplesheet import SampleSheet

class SampleSheetProjectFactory(IGFBaseJobFactory):
  '''
  A class for finding all the projects mentioned in the SampleSheet
  '''
  def param_defaults(self):
    params_dict=super(SampleSheetProjectFactory,self).param_defaults()
    params_dict.update({
        'singlecell_tag':'10X'
      })
    return params_dict


  def run(self):
    try:
      samplesheet_file=self.param_required('samplesheet')
      seqrun_igf_id=self.param_required('seqrun_igf_id')

      if not os.path.exists(samplesheet_file):
        raise IOError('Samplesheet file {0} not found'.format(samplesheet_file))

      samplesheet=SampleSheet(infile=samplesheet_file)                          # read samplesheet
      project_names=samplesheet.get_project_names()                             # get project names from samplesheet
      sub_tasks=[{'project_name':project_name} \
                 for project_name in project_names]                             # create subtasks data structure
      self.param('sub_tasks',sub_tasks)                                         # seed dataflow
      project_lane=samplesheet.get_project_and_lane()                           # get project name and lane info from samplesheet
      self.add_asana_notes(task_name=seqrun_igf_id,\
                           notes='\n'.join(project_lane))                       # add project names to asana notes
    except Exception as e:
      message = \
        'seqrun: {2}, Error in {0}: {1}'.\
          format(
            self.__class__.__name__,
            e,
            seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      self.post_message_to_ms_team(
          message=message,
          reaction='fail')
      raise