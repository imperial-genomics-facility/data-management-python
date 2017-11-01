import os, subprocess,json

class IGF_irods_uploader:
  '''
  A simple wrapper for uploading files to irods server from HPC cluster CX1
  Please run the following commands in the HPC cluster before running this module
  Add irods settings to ~/.irods/irods_environment.json
  > module load irods/4.2.0
  > iinit (optional username)
  Authenticate irods settings using your password
  The above command will generate a file containing your iRODS password in a 'scrambled form'
  '''
  def __init__(self,irods_exe_dir,host="eliot.med.ic.ac.uk",zone="/igfZone",
               port=1247,igf_user='igf', irods_resource='woolfResc'):
    self.irods_exe_dir=irods_exe_dir
    self.host=host
    self.zone=zone
    self.port=port
    self.igf_user=igf_user
    self.irods_resource=irods_resource


  def upload_fastqfile_and_create_collection(self,filepath,irods_user,project_name, \
                                             seqrun_igf_id,seqrun_date, \
                                             data_type='fastq'):
    '''
    A method for uploading files to irods server and creating collections with metadata
    '''
    try:
      if not os.path.exists(filepath) or os.path.isdir(filepath):
        raise IOError('filepath {0} not found or its not a file'.format(filepath))
      
      irods_exe_dir=self.irods_exe_dir
      file_name=os.path.basename(filepath)                                      # get file name
      irods_dir=os.path.join(self.zone,'home',irods_user,project_name,data_type,\
                             seqrun_date)                                       # configure destination path
      irods_filepath=os.path.join(irods_dir,file_name)                          # get the destination filepath
      file_meta_info='{0}-{1}-{2}'.format(seqrun_date,data_type,project_name)   # meatdata for irods file
      
      make_dir_cmd=['{0}/{1}'.format(irods_exe_dir,'imkdir'), '-p',irods_dir]
      subprocess.check_call(make_dir_cmd)                                       # create destination dir
      chmod_cmd=['{0}/{1}'.format(irods_exe_dir,'ichmod'),'-M','own', 
                 self.igf_user, irods_dir]
      subprocess.check_call(chmod_cmd)                                          # change directory ownership
      inherit_cmd=['{0}/{1}'.format(irods_exe_dir,'ichmod'),'-r', 'inherit', 
                   irods_dir]
      subprocess.check_call(inherit_cmd)                                        # inherit new directory
      imeta_add_cmd=['{0}/{1}'.format(irods_exe_dir,'imeta'),'add','-C',
                     irods_dir,'run_name', seqrun_igf_id]
      subprocess.check_call(imeta_add_cmd)                                      # set run_name metadata for new dir
      iput_cmd=['{0}/{1}'.format(irods_exe_dir,'iput'),'-k','-f','-N','1','-R',
                self.irods_resource, filepath, irods_dir]
      subprocess.check_call(iput_cmd)                                           # upload file to irods dir, calculate md5sub and overwrite
      meta_30d=['{0}/{1}'.format(irods_exe_dir,'isysmeta'),'mod', 
                irods_filepath,'"+30d"']
      subprocess.check_call(meta_30d)                                           # add metadata for file
      meta_project_user=['{0}/{1}'.format(irods_exe_dir,'imeta'),'add','-d',
                         irods_filepath,file_meta_info, irods_user,'iRODSUserTagging:Star']
      subprocess.check_call(meta_project_user)                                  # add more metadata to file
      meta_file_retentaion=['{0}/{1}'.format(irods_exe_dir,'imeta'),'add','-d',
                            irods_filepath, 'retention "30" "days"']
      subprocess.check_call(meta_file_retentaion)                               # adding file retaintion info
      
    except:
      raise


    