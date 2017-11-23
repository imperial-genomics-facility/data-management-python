import os, subprocess
from jinja2 import Template,Environment, FileSystemLoader
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.fileutils import get_temp_dir
from igf_data.utils.fileutils import copy_remote_file

class PrepareQcPageForRemote(IGFBaseProcess):
  '''
  Runnable module for creating remote qc page for project and samples.
  Also copy the static html page to remote server
  '''
  def param_defaults(self):
    params_dict=super(IGFBaseProcess,self).param_defaults()
    params_dict.update({
      'qc_template_path':'q_report',
      'project_template':'index.html',
      'sample_template':'sample_level_qc.html',
      'project_filename':'index.html',
      'sample_filename':'sampleQC.html',
      'remote_project_path':None,
      'remote_user':None,
      'remote_host':None,
      'lane_index_info':None
    })
    return params_dict
  
  def run(self):
    try:
      seqrun_igf_id=self.param_required('seqrun_igf_id')
      project_name=self.param_required('project_name')
      seqrun_date=self.param_required('seqrun_date')
      flowcell_id=self.param_required('flowcell_id')
      remote_project_path=self.param_required('remote_project_path')
      remote_user=self.param_required('remote_user')
      remote_host=self.param_required('remote_host')
      template_dir=self.param_required('template_dir')
      page_type=self.param_required('page_type')
      lane_index_info=self.param_required('lane_index_info') 
      qc_template_path=self.param('qc_template_path')
      project_template=self.param('project_template')
      sample_template=self.param('sample_template')
      project_filename=self.param('project_filename')
      sample_filename=self.param('sample_filename')
     
      
      if page_type not in ['project','sample']:
        raise ValueError('Project type {0} is not defined yet'.format(page_type))
      
      remote_file_path=os.path.join(remote_project_path,\
                                      project_name, \
                                      seqrun_date, \
                                      flowcell_id,\
                                      lane_index_info)                          # generic remote path, lane info is none for project
      
      if page_type == 'project':                                                # prepare project page
        template_file=os.path.join(template_dir,\
                                   qc_template_path,\
                                   project_template)

        
      elif page_type == 'sample':                                               # prepare sample page
        template_file=os.path.join(template_dir,\
                                   qc_template_path,\
                                   sample_template)
       
    except Exception as e:
      message='seqrun: {2}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise