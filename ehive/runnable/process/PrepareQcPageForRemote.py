import os, subprocess
import pandas as pd
from jinja2 import Template,Environment, FileSystemLoader
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.fileutils import get_temp_dir
from igf_data.utils.fastqc_utils import get_fastq_info_from_fastq_zip
from igf_data.utils.fileutils import copy_remote_file
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.runadaptor import RunAdaptor
from igf_data.illumina.samplesheet import SampleSheet

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
      'lane_index_info':None,
      'fastq_dir':None,
      'qc_files':None,
      'samplesheet_filename':'SampleSheet.csv',
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
      fastq_dir=self.param_required('fastq_dir')
      qc_files=self.param_required('qc_files')
      samplesheet_filename=self.param('samplesheet_filename')
      lane_index_info=self.param_required('lane_index_info')
      qc_template_path=self.param('qc_template_path')
      project_template=self.param('project_template')
      sample_template=self.param('sample_template')
      project_filename=self.param('project_filename')
      sample_filename=self.param('sample_filename')
     
      
      if page_type not in ['project','sample']:
        raise ValueError('Project type {0} is not defined yet'.format(page_type))
      
      qc_template_path=os.path.join(template_dir,qc_template_path)
      remote_file_path=os.path.join(remote_project_path,\
                                      project_name, \
                                      seqrun_date, \
                                      flowcell_id,\
                                      lane_index_info)                          # generic remote path, lane info is none for project
      
      template_env=Environment(loader=FileSystemLoader(searchpath=qc_template_path), \
                               autoescape=select_autoescape(['xml']))           # set template env
      
      remote_chk_cmd=['ssh',\
                      '{0}@{1}'.\
                      format(remote_user,\
                             remote_host),\
                      'ls']
      
      remote_rm_cmd=['ssh',\
                      '{0}@{1}'.\
                      format(remote_user,\
                             remote_host),\
                      'rm', \
                      '-f']
      
      temp_work_dir=get_temp_dir()                                              # get a temp dir
      
      report_output_file=None
      if page_type == 'project':                                                # prepare project page
        template_file=template_env.get_template(project_template)
        report_output_file=os.path.join(temp_work_dir,project_filename)
        template_file.\
        stream(ProjectName=project_name, \
               SeqrunDate=seqrun_date, \
               FlowcellId=flowcell_id, \
               headerdata=None, \
               qcmain=None, \
              ).\
        dump(report_output_file)
        os.chmod(report_output_file, mode=0o754)
        
        remote_chk_cmd.append(os.path.join(remote_file_path,project_filename))
        remote_rm_cmd.append(os.path.join(remote_file_path,project_filename))
        
      elif page_type == 'sample':                                               # prepare sample page
        if lane_index_info is None:
          raise ValueError('Missing lane and index information')
        
        (headerdata,qcmain)=self._process_samples_data()
        
        

        (lane_id,index_length)=lane_index_info.split('_',1)                     # get lane and index info
        
        
        template_file=template_env.get_template(sample_template)
        report_output_file=os.path.join(temp_work_dir,sample_filename)
        template_file.\
        stream(ProjectName=project_name, \
               SeqrunDate=seqrun_date, \
               FlowcellId=flowcell_id, \
               Lane=lane_id, \
               IndexBarcodeLength=index_length, \
               headerdata=None, \
               qcmain=None, \
              ).\
        dump(report_output_file)
        os.chmod(report_output_file, mode=0o754)
        
        remote_chk_cmd.append(os.path.join(remote_file_path,sample_filename))
        remote_rm_cmd.append(os.path.join(remote_file_path,sample_filename))
        
      response=subprocess.call(remote_chk_cmd)
      if response!=0:
        subprocess.check_call(remote_rm_cmd)                                    # remove existing remote file
      
      if not os.path.exists(report_output_file):
        raise IOError('file {0} not found'.format(report_output_file))
      
      copy_remote_file(source_path=report_output_file, \
                       destinationa_path=remote_file_path, \
                       destination_address='{0}@{1}'.format(remote_user,\
                                                            remote_host))       # copy file to remote
      self.param('dataflow_params',{'remote_qc_page':\
                                    os.path.join(remote_file_path,
                                                 os.path.basename(report_output_file))})
    except Exception as e:
      message='seqrun: {2}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise
    
    
  def _process_samples_data(self):
    '''
    An internal method for processing samples data
    '''
    try:
      fastq_dir=self.param_required('fastq_dir')
      qc_files=self.param_required('qc_files')
      samplesheet_filename=self.param('samplesheet_filename')
      igf_session_class=self.param_required('igf_session_class')
      remote_project_path=self.param_required('remote_project_path')
      project_name=self.param_required('project_name')
      seqrun_date=self.param_required('seqrun_date')
      flowcell_id=self.param_required('flowcell_id')
      lane_index_info=self.param_required('lane_index_info')
      
      remote_path=os.path.join(remote_project_path, \
                               project_name, \
                               seqrun_date, \
                               flowcell_id, \
                               lane_index_info)                                 # get remote base path
      
      base=BaseAdaptor(**{'session_class':igf_session_class})
      base.start_session()                                                      # connect to db
      
      ca=CollectionAdaptor(**{'session':base.session})
      ra=RunAdaptor(**{'session':base.session})
      
      fastqc_data=list()
      for fastqc_file in qc_files[fastq_dir]['fastqc']:                         # get fastqc files for fastq_dir
        fastqc_zip=fastqc_file['fastqc_zip']
        fastq_file=fastqs_file['fastq_file']
        remote_fastqc_path=fastqc_file['remote_fastqc_path']
        remote_fastqc_path=os.path.relpath(remote_fastqc_path, \
                                           start=remote_path)                   # get relative path
        (total_reads, fastq_filename)=get_fastq_info_from_fastq_zip(fastqc_zip)
        (collection_name,collection_table)=\
        ca.fetch_collection_name_and_table_from_file_path(file_path=fastqc_zip) # fetch collection name and table info
        sample=ra.fetch_sample_info_for_run(run_igf_id=collection_name)
        sample_name=sample['sample_igf_id']
        fastqc_data.append({'Sample_ID':sample_name,\
                            'Fastqc':remote_fastqc_path, \
                            'FastqFile':fastq_file, \
                            'TotalReads':total_reads})
        
      base.close_session()                                                      # close db connection
      fastqs_data=list()
      for fastqs_file in qc_files[fastq_dir]['fastqscreen']:                    # get fastqs files for fastq_dir
        fastq_file=fastqs_file['fastq_file']
        remote_fastqs_path=fastqs_file['remote_fastqc_path']
        remote_fastqs_path=os.path.relpath(remote_fastqs_path, \
                                           start=remote_path)                   # get relative path
        fastqs_data.append({'Fastqscreen':remote_fastqs_path, \
                            'FastqFile':fastq_file})
      
      if len(fastqc_data)==0 or len(fastqs_data)==0:
        raise ValueError('Value not found for fastqc: {0} or fastqscreen:{1}'.\
                         format(len(fastqc_data), len(fastqs_data)))
      
      fastqc_data=pd.DataFrame(fastqc_data)
      fastqs_data=pd.DataFrame(fastqs_data).set_index('FastqFile')              # convert to dataframe
      merged_qc_info=fastqc_data.join(fastqs_data, \
                                      how='inner', \
                                      on='FastqFile', \
                                      lsuffix='', \
                                      rsuffix='_s'
                                     )                                          # merge fastqc and fastqscreen info
      if len(merged_qc_info)==0:
        raise ValueError('No QC data found for merging, fastqc:{0}, fastqscreen: {1}'.\
                         format(len(fastqc_data), len(fastqs_data)))
      
      samplesheet_file=os.path.join(fastq_dir,samplesheet_filename)
      if not os.path.exists(samplesheet_file):
          raise IOError('samplesheet file {0} not found'.format(samplesheet_file))
        
      sa=SampleSheet(infile=samplesheet_file)
      sample_data=pd.DataFrame(sa._data).set_index('Sample_ID')                 # get sample infor from Samplesheet
      merged_data=merged_qc_info.join(sample_data, \
                                      how='inner', \
                                      on='Sample_ID', \
                                      lsuffix='', \
                                      rsuffix='_sa')                            # merge sample data with qc data
      required_headers=['Sample_ID',
                        'Sample_Name',
                        'TotalReads',
                        'index']
      if 'index2' in list(sample_data.columns):
        required_headers.append('index2')
        
      required_headers.extend(['Fastqc',
                               'Fastqscreen'])                                  # create header order
      qc_merged_data=merged_data.loc[:,required_headers].\
                                 to_dict(orient='records')                      #  extract final data
      return required_headers, qc_merged_data
    except:
      raise    