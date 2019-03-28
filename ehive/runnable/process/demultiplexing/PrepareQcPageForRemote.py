import os, subprocess, fnmatch
import pandas as pd
from jinja2 import Template,Environment, FileSystemLoader,select_autoescape
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
    params_dict=super(PrepareQcPageForRemote,self).param_defaults()
    params_dict.update({
      'qc_template_path':'qc_report',
      'project_template':'index.html',
      'undetermined_template':'undetermined.html',
      'sample_template':'sample_level_qc.html',
      'project_filename':'index.html',
      'sample_filename':'sampleQC.html',
      'undetermined_filename':'undetermined.html',
      'remote_project_path':None,
      'remote_user':None,
      'remote_host':None,
      'lane_index_info':None,
      'fastq_dir':None,
      'qc_files':None,
      'multiqc_remote_file':None,
      'samplesheet_filename':'SampleSheet.csv',
      'report_html':'*all/all/all/laneBarcode.html',
      'remote_ftp_base':'/www/html',
      'singlecell_tag':'10X',
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
      fastq_dir=self.param('fastq_dir')
      qc_files=self.param_required('qc_files')
      multiqc_remote_file=self.param('multiqc_remote_file')
      samplesheet_filename=self.param('samplesheet_filename')
      lane_index_info=self.param('lane_index_info')
      qc_template_path=self.param('qc_template_path')
      project_template=self.param('project_template')
      undetermined_template=self.param('undetermined_template')
      sample_template=self.param('sample_template')
      project_filename=self.param('project_filename')
      sample_filename=self.param('sample_filename')
      undetermined_filename=self.param('undetermined_filename')
      report_html=self.param('report_html')
      remote_ftp_base=self.param('remote_ftp_base')
      singlecell_tag=self.param('singlecell_tag')
     
      if page_type not in ['project','sample','undetermined']:
        raise ValueError('Project type {0} is not defined yet'.format(page_type))
      
      qc_template_path=os.path.join(template_dir,qc_template_path)
      remote_file_path=os.path.join(remote_project_path,\
                                      project_name, \
                                      seqrun_date, \
                                      flowcell_id,\
                                    )
      if lane_index_info is not None:
        remote_file_path=os.path.join(remote_file_path,
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
      qc_file_info=dict()
      qc_file_info.update({
        'project_name':project_name, \
        'flowcell': flowcell_id, \
        
        })
      if page_type=='project':                                                  # prepare project page
        (headerdata, qcmain)=self._process_projects_data()                      # get required data for project qc page
        
        template_file=template_env.get_template(project_template)
        report_output_file=os.path.join(temp_work_dir,project_filename)
        template_file.\
        stream(ProjectName=project_name, \
               SeqrunDate=seqrun_date, \
               FlowcellId=flowcell_id, \
               headerdata=headerdata, \
               qcmain=qcmain, \
              ).\
        dump(report_output_file)
        os.chmod(report_output_file, mode=0o754)
        
        remote_chk_cmd.append(os.path.join(remote_file_path,project_filename))
        remote_rm_cmd.append(os.path.join(remote_file_path,project_filename))
        
      elif page_type=='undetermined':                                           # prepare undetermined fastq page
        (headerdata, qcmain)=self._process_undetermined_data(remote_file_path)  # get required data for undetermined qc page
        template_file=template_env.get_template(undetermined_template)
        report_output_file=os.path.join(temp_work_dir,undetermined_filename)
        template_file.\
        stream(ProjectName=project_name, \
               SeqrunDate=seqrun_date, \
               FlowcellId=flowcell_id, \
               headerdata=headerdata, \
               qcmain=qcmain, \
              ).\
        dump(report_output_file)
        os.chmod(report_output_file, mode=0o754)
        remote_chk_cmd.append(os.path.join(remote_file_path,undetermined_filename))
        remote_rm_cmd.append(os.path.join(remote_file_path,undetermined_filename))
        
      elif page_type=='sample':                                                 # prepare sample page
        if lane_index_info is None:
          raise ValueError('Missing lane and index information')
      
        if fastq_dir is None:
            raise ValueError('Missing required fastq_dir')
        
        (headerdata, qcmain)=self._process_samples_data()                       # get required data for sample qc page
        (lane_id,index_length)=lane_index_info.split('_',1)                     # get lane and index info
        template_file=template_env.get_template(sample_template)                # get template file
        report_output_file=os.path.join(temp_work_dir,sample_filename)
        template_file.\
        stream(ProjectName=project_name, \
               SeqrunDate=seqrun_date, \
               FlowcellId=flowcell_id, \
               Lane=lane_id, \
               IndexBarcodeLength=index_length, \
               headerdata=headerdata, \
               qcmain=qcmain, \
              ).\
        dump(report_output_file)                                                # dump data to template file
        os.chmod(report_output_file, mode=0o754)
        
        remote_chk_cmd.append(os.path.join(remote_file_path,sample_filename))
        remote_rm_cmd.append(os.path.join(remote_file_path,sample_filename))
        
        remote_sample_qc_path=os.path.join(remote_file_path, \
                                           os.path.basename(report_output_file))
        if multiqc_remote_file is None:
          raise ValueError('required a valid path for remote multiqc')
        
        remote_path=os.path.join(remote_project_path, \
                                 project_name, \
                                 seqrun_date, \
                                 flowcell_id, \
                                 )                                              # get remote base path
        remote_sample_qc_path=os.path.relpath(remote_sample_qc_path, \
                                              start=remote_path)                # elative path for sample qc
        multiqc_remote_file=os.path.relpath(multiqc_remote_file, \
                                            start=remote_path)                  # relative path for multiqc
        
        report_htmlname=os.path.basename(report_html)
        reports=list()
        for root,dirs, files in os.walk(top=fastq_dir):
          if report_htmlname in files:
            reports.extend([os.path.join(os.path.abspath(root),file) \
                       for file in files \
                         if fnmatch.fnmatch(os.path.join(root,file),report_html)]) # get all html reports
        if len(reports)==0:
          raise ValueError('No demultiplexing report found for fastq dir {0}'.\
                           format(fastq_dir))
        os.chmod(reports[0], mode=0o774)                                        # added read permission for report html
        copy_remote_file(source_path=reports[0], \
                       destinationa_path=remote_file_path, \
                       destination_address='{0}@{1}'.format(remote_user,\
                                                            remote_host))       # copy file to remote
        remote_report_file=os.path.join(remote_file_path,\
                                        os.path.basename(reports[0]))           # get remote path for report file
        remote_report_file=os.path.relpath(remote_report_file, \
                                              start=remote_path)                # get relative path for demultiplexing report
        
        qc_file_info={'lane_id':lane_id, \
                      'index_length':index_length,
                      'sample_qc_page':remote_sample_qc_path, \
                      'multiqc_page':multiqc_remote_file, \
                      'demultiplexing_report':remote_report_file,\
                      'fastq_dir':fastq_dir, \
                      'project_name':project_name,
                     }
        
      response=subprocess.call(remote_chk_cmd)
      if response!=0:
        subprocess.check_call(remote_rm_cmd)                                    # remove existing remote file
      
      if not os.path.exists(report_output_file):
        raise IOError('file {0} not found'.format(report_output_file))
      
      copy_remote_file(source_path=report_output_file, \
                       destinationa_path=remote_file_path, \
                       destination_address='{0}@{1}'.format(remote_user,\
                                                            remote_host))       # copy file to remote
      remote_qc_page=os.path.join(remote_file_path, \
                                  os.path.basename(report_output_file))
      qc_file_info.update({'remote_qc_page':remote_qc_page})
      self.param('dataflow_params',{'qc_file_info':qc_file_info})
      
      remote_url_path='http://{0}/{1}'.format(remote_host,\
                                              os.path.relpath(remote_qc_page,\
                                                              start=remote_ftp_base))
      message='QC page {0}, {1},{2}: {3}'.\
              format(seqrun_igf_id, \
                     project_name, \
                     page_type, \
                     remote_url_path,
                    )
      self.post_message_to_slack(message,reaction='pass')                       # send msg to slack
      self.comment_asana_task(task_name=seqrun_igf_id, comment=message)         # send msg to asana
    except Exception as e:
      message='seqrun: {2}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise
    
    
  def _process_projects_data(self):
    '''
    An internal method for processing projects data
    '''
    qc_files=self.param_required('qc_files')
    project_name=self.param_required('project_name')
    
    required_header=['LaneID',\
                     'Index_Length',\
                     'MultiQC',\
                     'SampleQC',\
                     'Demultiplexing_Report',\
                    ]
    qc_data=list()
    for fastq_dir, sample_data in qc_files.items():
      sample_project=sample_data['project_name']
      if sample_project==project_name:                                          # additional checks for project name
        qc_data.append({'LaneID':str(sample_data['lane_id']),\
                        'Index_Length':str(sample_data['index_length']),\
                        'MultiQC':sample_data['multiqc_page'],\
                        'SampleQC':sample_data['sample_qc_page'],\
                        'Demultiplexing_Report':sample_data['demultiplexing_report'],\
                      })                                                        # adding data for qc page
    if len(qc_data) > 0:
      qc_data=pd.DataFrame(qc_data).\
              sort_values(by='LaneID').\
              to_dict(orient='records')                                         # sort qc data based on lane id
    return required_header, qc_data


  def _process_undetermined_data(self,remote_file_path):
    '''
    An internal method for processing undetermined data
    '''
    try:
      qc_files=self.param_required('qc_files')
      required_header=['LaneID',\
                       'Index_Length',\
                       'Undetermined_MultiQC',\
                      ]
      qc_data=list()
      for fastq_dir, remote_multiqc_path in qc_files.items():
        lane_index=os.path.basename(fastq_dir)
        (lane_id, index_length)=lane_index.split('_',1)
        remote_multiqc_path=os.path.relpath(remote_multiqc_path,\
                                            start=remote_file_path)            # get relative path for remote file
        qc_data.append({'LaneID':lane_id,\
                        'Index_Length':index_length,\
                        'Undetermined_MultiQC':remote_multiqc_path,\
                       })

      if len(qc_data) > 0:
        qc_data=pd.DataFrame(qc_data).\
              sort_values(by='LaneID').\
              to_dict(orient='records')                                         # sort qc data based on lane id

      return required_header, qc_data
    except:
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
      singlecell_tag=self.param('singlecell_tag')
      
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
      for fastqc_file in qc_files['fastqc']:                                    # get fastqc files for fastq_dir
        fastqc_zip=fastqc_file['fastqc_zip']
        fastq_file=fastqc_file['fastq_file']
        qc_fastq_dir=fastqc_file['fastq_dir']
        
        if qc_fastq_dir==fastq_dir:                                             # check for fastq dir
          remote_fastqc_path=fastqc_file['remote_fastqc_path']
          remote_fastqc_path=os.path.relpath(remote_fastqc_path, \
                                           start=remote_path)                   # get relative path
          (total_reads, fastq_filename)=get_fastq_info_from_fastq_zip(fastqc_zip)
          (collection_name,collection_table)=\
          ca.fetch_collection_name_and_table_from_file_path(file_path=fastq_file) # fetch collection name and table info
          sample=ra.fetch_sample_info_for_run(run_igf_id=collection_name)
          sample_name=sample['sample_igf_id']
          fastqc_data.append({'Sample_ID':sample_name,\
                              'Fastqc':remote_fastqc_path, \
                              'FastqFile':fastq_file, \
                              'TotalReads':total_reads})
        
      base.close_session()                                                      # close db connection
      fastqs_data=list()
      for fastqs_file in qc_files['fastqscreen']:                               # get fastqs files for fastq_dir
        fastq_file=fastqs_file['fastq_file']
        remote_fastqs_path=fastqs_file['remote_fastqscreen_path']
        qs_fastq_dir=fastqc_file['fastq_dir']
        
        if qs_fastq_dir==fastq_dir:                                             # check for accu data
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

      final_samplesheet_data=list()
      samplesheet_sc=SampleSheet(infile=samplesheet_file)                       # read samplesheet for single cell check
      samplesheet_sc.filter_sample_data(condition_key='Description', 
                                        condition_value=singlecell_tag, 
                                        method='include')                       # keep only single cell samples
      if len(samplesheet_sc._data) >0:
        sc_data=pd.DataFrame(samplesheet_sc._data).\
                   drop(['Sample_ID','Sample_Name','index'],axis=1).\
                   drop_duplicates().\
                   rename(columns={'Original_Sample_ID':'Sample_ID',
                                   'Original_Sample_Name':'Sample_Name',
                                   'Original_index':'index'}).\
                   to_dict(orient='region')                                     # restructure single cell data. sc data doesn't have index2
        final_samplesheet_data.extend(sc_data)                                  # add single cell samples to final data

      sa=SampleSheet(infile=samplesheet_file)
      sa.filter_sample_data(condition_key='Description', 
                                        condition_value=singlecell_tag, 
                                        method='exclude')                       # remove only single cell samples
      if len(sa._data)>0:
        final_samplesheet_data.extend(sa._data)                                 # add non single cell samples info to final data

      sample_data=pd.DataFrame(final_samplesheet_data).set_index('Sample_ID')   # get sample info from final data
      merged_data=merged_qc_info.join(sample_data, \
                                      how='inner', \
                                      on='Sample_ID', \
                                      lsuffix='', \
                                      rsuffix='_sa')                            # merge sample data with qc data
      required_headers=['Sample_ID',
                        'Sample_Name',
                        'FastqFile',
                        'TotalReads',
                        'index']
      if 'index2' in list(sample_data.columns):
        required_headers.append('index2')
        
      required_headers.extend(['Fastqc',
                               'Fastqscreen'])                                  # create header order
      merged_data['FastqFile']=merged_data['FastqFile'].\
                               map(lambda path: os.path.basename(path))         # keep only fastq filename
      qc_merged_data=merged_data.loc[:,required_headers].\
                                 to_dict(orient='records')                      #  extract final data
      return required_headers, qc_merged_data
    except:
      raise    