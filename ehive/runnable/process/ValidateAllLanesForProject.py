#!/usr/bin/env python
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
class ValidateAllLanesForProject(IGFBaseProcess):
  '''
  A class for checking barcode stats for all the flowcell lanes
  for a project. It will allow the project to the next dataflow step
  if all the lanes are qc passed. It will remove the fastq data for
  projects with failed qc stats and send messsage to slack channel
  '''
  def run(self):
    project_fastq=self.param_required('project_fastq')
    project_status='PASS'                                                       # default status is PASS
    for fastq_dir,qc_stats in project_fastq.items():
      if qc_stats=='FAIL':
        project_status='FAIL'                                               # mark project status as failed if any lane is failed
        
    if project_status=='PASS':
    elif project_status=='FAIL':
    else:
        raise
          