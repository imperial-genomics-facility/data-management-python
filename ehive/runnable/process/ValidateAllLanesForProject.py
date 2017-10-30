#!/usr/bin/env python
import os
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from shutil import rmtree

class ValidateAllLanesForProject(IGFBaseProcess):
  '''
  A class for checking barcode stats for all the flowcell lanes
  for a project. It will allow the project to the next dataflow step
  if all the lanes are qc passed. It will remove the fastq data for
  projects with failed qc stats and send messsage to slack channel
  '''
  def param_defaults(self):
    params_dict=super(IGFBaseProcess,self).param_defaults()
    params_dict.update({
        'strict_check':True,
      })
    return params_dict
  
  def run(self):
    try:
      project_fastq=self.param_required('project_fastq')
      strict_check=self.param('strict_check')
      seqrun_igf_id=self.param_required('seqrun_igf_id')
      project_name=self.param_required('project_name')
      
      project_status='PASS'                                                     # default status is PASS
      for fastq_dir,qc_stats in project_fastq.items():
        if qc_stats=='FAIL':
          project_status='FAIL'                                                 # mark project status as failed if any lane is failed
          
      if project_status=='PASS':
        self.param('dataflow_params',{'project_fastq':project_fastq,
                                      'project_status':project_status})
      else:
        self.param('dataflow_params',{'project_status':project_status})
        
        for fastq_dir in project_fastq.keys():
          report_dir=os.path.join(fastq_dir,'Reports','html')
          for flowcell in os.listdir(report_dir):
            flowcell_dir=os.path.join(report_dir,flowcell)
            if os.path.isdir(flowcell_dir):
              all_barcodes_html=os.path.join(flowcell_dir,'all','all','all','laneBarcode.html')
              if os.path.exists(all_barcodes_html):
                message='Failed barcode stats for seqrun: {0}, project: {1}'.\
                         format(seqrun_igf_id,project_name)
                self.post_file_to_slack(filepath=all_barcodes_html,\
                                        message=message)                        # post report file to slack
                self.upload_file_to_asana_task(task_name=seqrun_igf_id, \
                                               filepath=all_barcodes_html,\
                                               comment=message)                 # attach html page to asana ticket
          if strict_check:
            self.post_message_to_slack(message='removing fastq dir {0}'.\
                                               format(fastq_dir), \
                                      reaction='pass')
            rmtree(fastq_dir)                                                   # delete failed fastq dir
    except Exception as e:
      message='seqrun: {2}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise