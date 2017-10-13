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
  def __init__(self,host="eliot.med.ic.ac.uk",zone="igfZone",port=1247,\
               igf_user='igf', irods_resource='woolfResc',irods_module='irods/4.2.0'):
    self.host=host
    self.zone=zone
    self.port=port
    self.igf_user=igf_user
    self.irods_resource=irods_resource
    self.irods_module=irods_module


  def upload_fastqfile_and_create_collection(self,filepath,irods_user,project_name, \
                                             seqrun_igf_id,seqrun_date, \
                                             data_type='fastq'):
    '''
    A method for uploading files to irods server and creating collections with metadata
    '''
    try:
      if not os.path.exists(path)(filepath) or not os.path.isdir(filepath):
        raise IOError('filepath {0} not found or its not a file'.format(filepath))
      
      file_name=os.path.basename(filepath)                                      # get file name
      self._load_irods_module_for_cx1()                                         # load irods env settings
      irods_dir=os.path.join(self.zone,'home',irods_user,project_name,data_type,\
                             seqrun_date)                                       # configure destination path
      irods_filepath=os.path.join(irods_dir,file_name)                          # get the destination filepath
      file_meta_info='{0}-{1}-{2}'.format(seqrun_date,data_type,project_name)   # meatdata for irods file
      
      subprocess.check_call(['imkdir','-p',irods_dir])                          # create destination dir
      subprocess.check_call(['ichmod','-M','own',self.igf_user, irods_dir])     # change directory ownership
      subprocess.check_call(['ichmod','-r', 'inherit', irods_dir])              # inherit new directory
      subprocess.check_call(['imeta','add','-C',irods_dir,'run_name',\
                             seqrun_igf_id])                                    # set run_name metadata for new dir
      subprocess.check_call(['iput','-k','-f','-N',1,'-R',self.irods_resource,\
                             filepath, irods_dir])                              # upload file to irods dir, calculate md5sub and overwrite
      subprocess.check_call(['isysmeta','mod', irods_filepath,'"+30d"'])        # add metadata for file
      subprocess.check_call(['imeta','add','-d',irods_filepath,file_meta_info, \
                             irods_user,'iRODSUserTagging:Star'])               # add more metadata to file
      subprocess.check_call(['imeta','add','-d',irods_filepath,\
                             'retention "30" "days"'])                          # adding file retaintion info
      
    except:
      raise
    
  
  def _load_irods_module_for_cx1(self):
    '''
    An internal module for loading irods setting for cx1 cluster
    '''
    try:
      subprocess.check_call(['module','load',self.irods_module])
    except:
      raise

    