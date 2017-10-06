#!/usr/bin/env python
import os
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from shutil import rmtree
from igf_data.task_tracking import igf_asana, igf_slack

class ValidateAllLanesForProject(IGFBaseProcess):
  '''
  A class for checking barcode stats for all the flowcell lanes
  for a project. It will allow the project to the next dataflow step
  if all the lanes are qc passed. It will remove the fastq data for
  projects with failed qc stats and send messsage to slack channel
  '''
  def param_defaults(self):
    params_dict=IGFBaseProcess.param_defaults()
    params_dict.update({
        'strict_check':True,
      })
    return params_dict
  
  def run(self):
    try:
      project_fastq=self.param_required('project_fastq')
      strict_check=self.param('strict_check')
      project_status='PASS'                                                     # default status is PASS
      for fastq_dir,qc_stats in project_fastq.items():
        if qc_stats=='FAIL':
          project_status='FAIL'                                                 # mark project status as failed if any lane is failed
          
      if project_status=='PASS':
        self.param('dataflow_params',{'project_fastq':project_fastq})
      else:
        for fastq_dir in project_fastq.key():
          report_dir=os.path.join(fastq_dir,'Reports/html')
          for flowcell in os.listdir(report_dir):
            if os.path.isdir(flowcell):
              all_barcodes_html=os.path.join(flowcell,'all/all/all/laneBarcode.html')
              if os.path.exists(all_barcodes_html):
                igf_slack.post_file_to_channel(filepath=all_barcodes_html, \
                                               message='Failed barcode stats')  # post report file to slack
          if strict_check:      
            rmtree(fastq_dir)                                                   # delete failed fastq dir
    except:
      raise