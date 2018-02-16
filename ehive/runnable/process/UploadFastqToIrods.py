#!/usr/bin/env python
import os,tarfile,fnmatch,datetime
from shutil import copy2
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from igf_data.utils.igf_irods_client import IGF_irods_uploader
from igf_data.igfdb.projectadaptor import ProjectAdaptor

class UploadFastqToIrods(IGFBaseProcess):
  def param_defaults(self):
    params_dict=super(UploadFastqToIrods,self).param_defaults()
    params_dict.update({
        'samplesheet_filename':'SampleSheet.csv',
        'report_html':'*all/all/all/laneBarcode.html',
        'irods_exe_dir':None,
      })
    return params_dict

  def run(self):
    try:
      fastq_dir=self.param_required('fastq_dir')
      seqrun_igf_id=self.param_required('seqrun_igf_id')
      project_name=self.param_required('project_name')
      igf_session_class=self.param_required('igf_session_class')
      irods_exe_dir=self.param_required('irods_exe_dir')
      flowcell_id=self.param_required('flowcell_id')
      samplesheet_filename=self.param('samplesheet_filename')
      manifest_name=self.param_required('manifest_name')
      report_html=self.param('report_html')

      pa=ProjectAdaptor(**{'session_class':igf_session_class})
      pa.start_session()
      user_info=pa.get_project_user_info(project_igf_id=project_name)         # fetch user info from db
      pa.close_session()

      user_info=user_info[user_info['data_authority']=='T']                     # filter dataframe for data authority
      user_info=user_info.to_dict(orient='records')                             # convert dataframe to list of dictionaries
      if len(user_info) == 0:
        raise ValueError('No user found for project {0}'.format(project_name)) 

      user_info=user_info[0]
      username=user_info['username']                                            # get username for irods

      report_htmlname=os.path.basename(report_html)
      seqrun_date=seqrun_igf_id.split('_')[0]                                   # collect seqrun date from igf id
      seqrun_date=datetime.datetime.strptime(seqrun_date,'%y%m%d').date()       # identify actual date
      seqrun_date=str(seqrun_date)                                              # convert object to string
      irods_upload=IGF_irods_uploader(irods_exe_dir)                            # create instance for irods upload
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
            tmp_samplesheet_file=os.path.join(temp_work_dir,\
                                              '{0}_{1}_{2}_{3}'.\
                                              format(project_name,\
                                                     base_seq_dir,\
                                                     seqrun_date,
                                                     samplesheet_filename))
            copy2(samplesheet_file, tmp_samplesheet_file)                       # change samplesheet filename
            self.post_message_to_slack(message=tmp_samplesheet_file,\
                                       reaction='pass')
            tar.add(tmp_samplesheet_file,\
                    arcname=os.path.relpath(tmp_samplesheet_file,\
                                            start=temp_work_dir))               # add samplesheet file to tar

          if report_htmlname in files:
            for file in files:
              if fnmatch.fnmatch(os.path.join(root,file),report_html):
                reports=os.path.join(os.path.abspath(root),file)                # get filepath for the report
                tmp_report_file=os.path.join(temp_work_dir,\
                                             '{0}_{1}_{2}_{3}'.\
                                             format(project_name,\
                                                    base_seq_dir,\
                                                    seqrun_date,\
                                                    os.path.basename(reports[0]))) # change report name
                copy2(os.path.join(os.path.abspath(root),\
                                   reports[0]),\
                      tmp_report_file)                                          # copy report file to temp
                self.post_message_to_slack(message=tmp_report_file,\
                                       reaction='pass')
                tar.add(tmp_report_file,\
                        arcname=os.path.relpath(tmp_report_file,\
                                                start=temp_work_dir))           # add demultiplexing report to tar

          if manifest_name in files:
            manifest_file=os.path.join(os.path.abspath(root),\
                                       manifest_name)                           # get samplesheet filepath
            tmp_manifest_file=os.path.join(temp_work_dir,\
                                           '{0}_{1}_{2}_{3}'.\
                                           format(project_name,\
                                                  base_seq_dir,\
                                                  seqrun_date,\
                                                  manifest_name))               # change manifest name
            copy2(manifest_file,tmp_manifest_file)                              # copy manifest to temp
            self.post_message_to_slack(message=tmp_manifest_file,\
                                       reaction='pass')
            tar.add(tmp_manifest_file,\
                    arcname=os.path.relpath(tmp_manifest_file,\
                                            start=temp_work_dir))               # add samplesheet file to tar

          for file in files:
            if fnmatch.fnmatch(file, '*.fastq.gz') and \
              not fnmatch.fnmatch(file, 'Undetermined_*'):
              fastq_file_path=os.path.join(os.path.abspath(root),file)          # get filepath for the fastq files
              tar.add(fastq_file_path,arcname=os.path.relpath(fastq_file_path,\
                                                              start=fastq_dir)) # add fastq file to tar

      irods_upload.\
      upload_fastqfile_and_create_collection(filepath=tarfile_name,\
                                             irods_user=username, \
                                             project_name=project_name, \
                                             run_igf_id=seqrun_igf_id, \
                                             flowcell_id=flowcell_id, \
                                             run_date=seqrun_date,\
                                            )                                   # upload fastq data to irods
      remove_dir(temp_work_dir)                                                 # remove temp dir once data uoload is done
    except Exception as e:
      message='seqrun: {2}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise