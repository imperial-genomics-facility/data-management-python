#!/usr/bin/env python
import os
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from shutil import rmtree
from igf_data.igfdb.projectadaptor import BaseAdaptor
from igf_data.igfdb.igfTables import Project, Project_attribute

class ValidateAllLanesForProject(IGFBaseProcess):
  '''
  A class for checking barcode stats for all the flowcell lanes
  for a project. It will allow the project to the next dataflow step
  if all the lanes are qc passed. It will remove the fastq data for
  projects with failed qc stats and send messsage to slack channel
  '''
  def param_defaults(self):
    params_dict=super(ValidateAllLanesForProject,self).param_defaults()
    params_dict.update({
        'strict_check':True,
      })
    return params_dict

  @staticmethod
  def _check_project_table(igf_session_class,project_name,project_status,barcode_check_key='barcode_check'):
    '''
    A static method for check project barcode check setting from database
    required params:
    igf_session_class: A database session class
    project_name: Name of the project
    project_status: Project status info, PASS or FAIL
    barcode_check_key: The attribute key name for barcode check, default barcode_check
    '''
    try:
      db_connected=0
      if project_status=='FAIL':
        base = BaseAdaptor(**{'session_class':igf_session_class})
        base.start_session()
        db_connected=1
        query=base.session.\
              query(Project).\
              join(Project_attribute).\
              filter(Project.project_id==Project_attribute.project_id).\
              filter(Project.project_name==project_name).\
              filter(Project_attribute.attribute_name==barcode_check_key)       # seq query for db lookup
        result=base.fetch_records(query, output_mode='one_or_none')             # fetch data from db
        if result is not None and result.attribute_value=='OFF':                # checking for barcode checking status
          project_status=='PASS'                                                # reset project status
      return project_status
    except:
      raise
    finally:
      if db_connected==1:
        base.close_session()

  def run(self):
    try:
      project_fastq=self.param_required('project_fastq')
      strict_check=self.param('strict_check')
      seqrun_igf_id=self.param_required('seqrun_igf_id')
      project_name=self.param_required('project_name')
      igf_session_class=self.param_required('igf_session_class')

      project_status='PASS'                                                     # default status is PASS
      for fastq_dir,qc_stats in project_fastq.items():
        if qc_stats=='FAIL':
          project_status='FAIL'                                                 # mark project status as failed if any lane is failed

      project_status=self._check_project_table(igf_session_class,\
                                               project_name,\
                                               project_status)                  # override project status check of dodgy projects 
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