import os, subprocess,json
from igf_data.utils.fileutils import get_datestamp_label

class IGF_irods_uploader:
  '''
  A simple wrapper for uploading files to irods server from HPC cluster CX1
  Please run the following commands in the HPC cluster before running this module
  Add irods settings to ~/.irods/irods_environment.json
  > module load irods/4.2.0
  > iinit (optional username)
  Authenticate irods settings using your password
  The above command will generate a file containing your iRODS password in a 'scrambled form'
  
  :param irods_exe_dir: A path to the bin directory where icommands are installed
  '''
  def __init__(self,irods_exe_dir,host="eliot.med.ic.ac.uk",zone="/igfZone",
               port=1247,igf_user='igf', irods_resource='woolfResc'):
    self.irods_exe_dir=irods_exe_dir
    self.host=host
    self.zone=zone
    self.port=port
    self.igf_user=igf_user
    self.irods_resource=irods_resource


  def upload_analysis_results_and_create_collection(self,file_list,irods_user,
                                                    project_name,analysis_name='default',
                                                    dir_path_list=None,file_tag=None):
    '''
    A method for uploading analysis files to irods server
    
    :param file_list: A list of file paths to upload to irods
    :param irods_user: Irods user name
    :param project_name: Name of the project_name
    :param analysis_name: A string for analysis name, default is 'default'
    :param dir_path_list: A list of directory structure for irod server, default None for using datestamp
    :param file_tag: A text string for adding tag to collection, default None for only project_name
    '''
    try:
      irods_exe_dir=self.irods_exe_dir
      irods_base_dir=os.path.join(self.zone, \
                                  'home', \
                                  irods_user, \
                                  project_name)
      if dir_path_list is not None and \
         isinstance(dir_path_list, list) and \
         len(dir_path_list) >0 :
        irods_base_dir=os.path.join(irods_base_dir,
                                    os.path.sep.join(dir_path_list))            # use path from dir list
      else:
        datestamp=get_datestamp_label()
        irods_base_dir=os.path.join(irods_base_dir,
                                    datestamp)                                  # use datestamp

      irods_base_dir=os.path.join(irods_base_dir,
                                  analysis_name)                                # add analysis name to the irods dir
      chk_cmd=[os.path.join(irods_exe_dir,'ils'),
               irods_base_dir]
      response=subprocess.call(chk_cmd)                                         # check for existing dir in irods
      if response != 0:                                                         # create dir if response is not 0
        make_dir_cmd=[os.path.join(irods_exe_dir,'imkdir'),
                      '-p',
                      irods_base_dir]
        subprocess.check_call(make_dir_cmd)                                     # create destination dir
        chmod_cmd=[os.path.join(irods_exe_dir,'ichmod'),
                   '-M',
                   'own',
                   self.igf_user, \
                   irods_base_dir]
        subprocess.check_call(chmod_cmd)                                        # change directory ownership
        inherit_cmd=[os.path.join(irods_exe_dir,'ichmod'),
                     '-r',
                     'inherit',
                     irods_base_dir]
        subprocess.check_call(inherit_cmd)                                      # inherit new directory

      for filepath in file_list:
        if not os.path.exists(filepath) or os.path.isdir(filepath):
          raise IOError('filepath {0} not found or its not a file'.\
                        format(filepath))                                       # checking filepath before upload

        irods_filepath=os.path.join(irods_base_dir,
                                    os.path.basename(filepath))
        file_chk_cmd=[os.path.join(irods_exe_dir,'ils'),
                      irods_filepath]
        file_response=subprocess.call(file_chk_cmd)                             # check for existing file in irods
        if file_response==0:
          file_rm_cmd=[os.path.join(irods_exe_dir,'irm'),
                     '-rf',
                     irods_filepath]
          subprocess.check_call(file_rm_cmd)                                    # remove existing file to prevent any clash

        iput_cmd=[os.path.join(irods_exe_dir,'iput'),
                  '-k',
                  '-f',
                  '-N','1',
                  '-R',
                  self.irods_resource,
                  filepath,
                  irods_base_dir]
        subprocess.check_call(iput_cmd)                                         # upload file to irods dir, calculate md5sub and overwrite
        if file_tag is None:
          file_meta_info=project_name
        else:
          file_meta_info='{0} - {1}'.format(project_name,
                                            file_tag)

        meta_project_user=[os.path.join(irods_exe_dir,'imeta'),
                           'add',
                           '-d',
                           irods_filepath,\
                           file_meta_info, \
                           irods_user,\
                           'iRODSUserTagging:Star']
        subprocess.check_call(meta_project_user)                                # add more metadata to file
        meta_30d=[os.path.join(irods_exe_dir,'isysmeta'),
                  'mod',
                  irods_filepath,
                  '"+30d"']
        subprocess.call(meta_30d)                                               # add metadata for file
        meta_file_retentaion=[os.path.join(irods_exe_dir,'imeta'),
                              'add',
                              '-d',
                              irods_filepath,
                              'retention "30" "days"']
        subprocess.call(meta_file_retentaion)                                   # adding file retaintion info
    except:
      raise


  def upload_fastqfile_and_create_collection(self,filepath,irods_user,project_name, \
                                             run_igf_id, run_date, flowcell_id=None, \
                                             data_type='fastq'):
    '''
    A method for uploading files to irods server and creating collections with metadata
    
    :param filepath: A file for upload to iRODS server
    :param irods_user: Recipient user's irods username
    :param project_name: Name of the project. This will be user for collection tag
    :param run_igf_id: A unique igf id, either seqrun or run or experiment
    :param run_date: A unique run date
    :param data_type: A directory label, e.g, fastq, bam or cram 
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
      meta_30d=['{0}/{1}'.format(irods_exe_dir,'isysmeta'),\
                'mod', \
                irods_filepath,\
                '"+30d"']
      subprocess.call(meta_30d)                                                 # add metadata for file
      meta_project_user=['{0}/{1}'.format(irods_exe_dir,'imeta'),\
                         'add',\
                         '-d',\
                         irods_filepath,\
                         file_meta_info, \
                         irods_user,\
                         'iRODSUserTagging:Star']
      subprocess.check_call(meta_project_user)                                  # add more metadata to file
      meta_file_retentaion=['{0}/{1}'.format(irods_exe_dir,'imeta'),
                            'add',
                            '-d',
                            irods_filepath, 'retention "30" "days"']
      subprocess.call(meta_file_retentaion)                                     # adding file retaintion info
      
    except:
      raise


    