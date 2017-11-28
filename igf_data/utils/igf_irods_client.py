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
  required params:
  irods_exe_dir: A path to the bin directory where icommands are installed
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
                                             run_igf_id, run_date, flowcell_id=None, \
                                             data_type='fastq'):
    '''
    A method for uploading files to irods server and creating collections with metadata
    required params:
    filepath: A file for upload to iRODS server
    irods_user: Recipient user's irods username
    project_name: Name of the project. This will be user for collection tag
    run_igf_id: A unique igf id, either seqrun or run or experiment
    run_date: A unique run date
    data_type: A directory label, e.g, fastq, bam or cram 
    '''
    try:
      if not os.path.exists(filepath) or os.path.isdir(filepath):
        raise IOError('filepath {0} not found or its not a file'.format(filepath))
      
      irods_exe_dir=self.irods_exe_dir
      file_name=os.path.basename(filepath)                                      # get file name
      irods_dir=os.path.join(self.zone, \
                             'home', \
                             irods_user, \
                             project_name, \
                             data_type, \
                             run_date)                                          # configure destination path
      if flowcell_id is not None:
        irods_dir=os.path.join(irods_dir,flowcell_id)                           # adding flowcell id to fastq path
        
      irods_filepath=os.path.join(irods_dir,file_name)                          # get the destination filepath
      file_meta_info='{0}-{1}-{2}'.format(run_date,\
                                          data_type,\
                                          project_name)                         # meatdata for irods file
      chk_cmd=['{0}/{1}'.format(irods_exe_dir,'ils'), \
               irods_dir]
      response=subprocess.call(chk_cmd)                                         # check for existing dir in irods
      if response != 0:                                                         # create dir if response is not 0
        make_dir_cmd=['{0}/{1}'.format(irods_exe_dir,'imkdir'), \
                      '-p',\
                      irods_dir]
        subprocess.check_call(make_dir_cmd)                                     # create destination dir
        chmod_cmd=['{0}/{1}'.format(irods_exe_dir,'ichmod'),\
                   '-M',\
                   'own',\
                   self.igf_user, \
                   irods_dir]
        subprocess.check_call(chmod_cmd)                                        # change directory ownership
        inherit_cmd=['{0}/{1}'.format(irods_exe_dir,'ichmod'),\
                     '-r', \
                     'inherit', \
                     irods_dir]
        subprocess.check_call(inherit_cmd)                                      # inherit new directory
        imeta_add_cmd=['{0}/{1}'.format(irods_exe_dir,'imeta'),\
                       'add',\
                       '-C',\
                       irods_dir,\
                       'run_name', \
                       run_igf_id]
        subprocess.check_call(imeta_add_cmd)                                    # set run_name metadata for new dir
        
      file_chk_cmd=['{0}/{1}'.format(irods_exe_dir,'ils'), \
                    irods_filepath]
      file_response=subprocess.call(file_chk_cmd)                               # check for existing file in irods
      
      if file_response==0:
        file_rm_cmd=['{0}/{1}'.format(irods_exe_dir,'irm'), \
                     '-rf', \
                     irods_filepath]
        subprocess.check_call(file_rm_cmd)                                      # remove existing file to prevent any clash
        
      iput_cmd=['{0}/{1}'.format(irods_exe_dir,'iput'),\
                '-k',\
                '-f',\
                '-N','1',\
                '-R',\
                self.irods_resource, \
                filepath, \
                irods_dir]
      subprocess.check_call(iput_cmd)                                           # upload file to irods dir, calculate md5sub and overwrite
     # meta_30d=['{0}/{1}'.format(irods_exe_dir,'isysmeta'),\
     #           'mod', \
     #           irods_filepath,\
     #           '"+30d"']
     # subprocess.check_call(meta_30d)                                           # add metadata for file
      meta_project_user=['{0}/{1}'.format(irods_exe_dir,'imeta'),\
                         'add',\
                         '-d',\
                         irods_filepath,\
                         file_meta_info, \
                         irods_user,\
                         'iRODSUserTagging:Star']
      subprocess.check_call(meta_project_user)                                  # add more metadata to file
      #meta_file_retentaion=['{0}/{1}'.format(irods_exe_dir,'imeta'),'add','-d',
      #                      irods_filepath, 'retention "30" "days"']
      #subprocess.check_call(meta_file_retentaion)                               # adding file retaintion info
      
    except:
      raise


    