#!/usr/bin/env python
import os,tarfile,fnmatch,datetime
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from igf_data.utils.igf_irods_client import IGF_irods_uploader
from igf_data.igfdb.projectadaptor import ProjectAdaptor

class UploadFastqToIrods(IGFBaseProcess):
  def param_defaults(self):
    params_dict=IGFBaseProcess.param_defaults()
    params_dict.update({
        'samplesheet_filename':'SampleSheet.csv',
        'report_html':'*all/all/all/laneBarcode.html',
      })
    return params_dict
  
  def run(self):
    try:
      fastq_dir=self.param_required('fastq_dir')
      seqrun_igf_id=self.param_required('seqrun_igf_id')
      project_igf_id=self.param_required('project_igf_id')
      igf_session_class=self.param_required('igf_session_class')
      samplesheet_filename=self.param('samplesheet_filename')
      report_html=self.param('report_html')
      
      pa=ProjectAdaptor(**{'session_class':igf_session_class})
      pa.start_session()
      user_info=pa.get_project_user_info(project_igf_id=project_igf_id)         # fetch user info from db
      pa.close_session()
      
      user_info=user_info[user_info['data_authority']=='T']                     # filter dataframe for data authority
      
      if len(user_info.index) == 0:
        raise ValueError('No user found for project {0}'.format(project_igf_id)) 
    
      username=user_info['username'].value[0]                                   # get username for irods
      
      report_htmlname=os.path.basename(report_html)
      seqrun_date=seqrun_igf_id.split('_')[0]                                   # collect seqrun date from igf id
      seqrun_date=datetime.datetime.strptime(seqrun_date,'%y%m%d').date()       # identify actual date
      seqrun_date=str(seqrun_date)                                              # convert object to string
      irods_upload=IGF_irods_uploader()                                         # create instance for irods upload
      base_seq_dir=os.path.basename(fastq_dir)                                  # get base name for the source dir
      tarfile_name='{0}_{1}_{2}.tar'.format(project_name,base_seq_dir,\
                                            seqrun_date)                        # construct name of the tarfile
      temp_work_dir=get_temp_dir()                                              # get a temp dir
      tarfile_name=os.path.join(temp_work_dir,tarfile_name)                     # create tarfile in the temp dir
      
      with tarfile.open(tarfile_name, "w") as tar:
        for root,dirs, files in os.walk(top=fastq_dir):
          if samplesheet_filename in files:
            samplesheet_file=os.path.join(os.path.abspath(root),\
                                          samplesheet_filename)                 # get samplesheet filepath
            tar.add(samplesheet_file,arcname=os.path.relpath(samplesheet_file,\
                                                             start=fastq_dir))  # add samplesheet file to tar
        
          if report_htmlname in files:
            for file in files:
              if fnmatch.fnmatch(os.path.join(root,file),report_html):
                reports=os.path.join(os.path.abspath(root),file)                # get filepath for the report
                tar.add(reports,arcname=os.path.relpath(reports[0],\
                                                        start=fastq_dir))       # add demultiplexing report to tar
        
          for file in files:
            if fnmatch.fnmatch(file, '*.fastq.gz'):
              fastq_file_path=os.path.join(os.path.abspath(root),file)          # get filepath for the fastq files
              tar.add(fastq_file_path,arcname=os.path.relpath(fastq_file_path,\
                                                              start=fastq_dir)) # add fastq file to tar
              
      irods_upload.upload_fastqfile_and_create_collection(filepath=tarfile_name,\
                                                          irods_user=username, \
                                                          project_name=project_igf_id, \
                                                          seqrun_igf_id=seqrun_igf_id, \
                                                          seqrun_date=seqrun_date,\
                                                          )                     # upload fastq data to irods
      remove_dir(temp_work_dir)                                                 # remove temp dir once data uoload is done
    except:
      raise