import os, subprocess, math
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.utils.fileutils import get_temp_dir, remove_dir
from igf_data.utils.fileutils import copy_remote_file
from jinja2 import Template,Environment, FileSystemLoader, select_autoescape


class CreateRemoteAccessForProject(IGFBaseProcess):
  '''
  Runnable module for creating remote directory for project and copy the htaccess
  file for user access
  '''
  def param_defaults(self):
    params_dict=super(CreateRemoteAccessForProject,self).param_defaults()
    params_dict.update({
      'htaccess_template_path':'ht_access',
      'htaccess_template':'htaccess.jinja',
      'htpasswd_template':'htpasswd.jinja',
      'htaccess_filename':'.htaccess',
      'htpasswd_filename':'.htpasswd',
      'project_template':'project_info/index.html',
      'status_template':'project_info/status.html',
      'analysis_template':'project_info/analysis.html',
      'analysis_viewer_template':'project_info/analysis_viewer.html',
      'remote_project_path':None,
      'remote_user':None,
      'remote_host':None,
      'seqruninfofile':'seqruninfofile.json',
      'samplereadcountfile':'samplereadcountfile.json',
      'samplereadcountcsvfile':'samplereadcountfile.csv',
      'status_data_json':'status_data.json',
      'analysis_data_json':'analysis_data.json',
      'analysis_data_csv':'analysis_data.csv',
      'analysis_view_js':'viewer.js',
      'image_height':700,
      'sample_count_threshold':75,
    })
    return params_dict

  def run(self):
    try:
      seqrun_igf_id=self.param_required('seqrun_igf_id')
      project_name=self.param_required('project_name')
      remote_project_path=self.param_required('remote_project_path')
      remote_user=self.param_required('remote_user')
      remote_host=self.param_required('remote_host')
      template_dir=self.param_required('template_dir')
      igf_session_class=self.param_required('igf_session_class')
      htaccess_template_path=self.param('htaccess_template_path')
      htaccess_template=self.param('htaccess_template')
      htpasswd_template=self.param('htpasswd_template')
      htaccess_filename=self.param('htaccess_filename')
      htpasswd_filename=self.param('htpasswd_filename')
      project_template=self.param('project_template')
      status_template=self.param('status_template')
      analysis_template=self.param('analysis_template')
      analysis_viewer_template=self.param('analysis_viewer_template')
      seqruninfofile=self.param('seqruninfofile')
      samplereadcountfile=self.param('samplereadcountfile')
      samplereadcountcsvfile=self.param('samplereadcountcsvfile')
      status_data_json=self.param('status_data_json')
      analysis_data_json=self.param('analysis_data_json')
      analysis_data_csv=self.param('analysis_data_csv')
      analysis_view_js=self.param('analysis_view_js')
      image_height=self.param('image_height')
      sample_count_threshold=self.param('sample_count_threshold')

      htaccess_template_path=\
          os.path.join(template_dir,
                       htaccess_template_path)                                  # set path for template dir
      project_template_path=\
          os.path.join(template_dir,
                       project_template)                                        # set path for project template
      status_template_path=\
          os.path.join(template_dir,
                       status_template)                                         # set path for project status template
      analysis_template_path=\
          os.path.join(template_dir,
                       analysis_template)                                       # set path for project analysis template
      analysis_viewer_template=\
          os.path.join(template_dir,
                       analysis_viewer_template)                                # set path for analysis viewer template
      pa=ProjectAdaptor(**{'session_class':igf_session_class})
      pa.start_session()
      user_info=pa.get_project_user_info(project_igf_id=project_name)           # fetch user info from db
      sample_counts=pa.count_project_samples(\
                        project_igf_id=project_name,
                        only_active=True)                                       # get sample counts for the project
      pa.close_session()

      image_height=\
          self._calculate_image_height(\
              sample_count=sample_counts,
              height=image_height,
              threshold=sample_count_threshold)                                 # change image height based on sample count

      user_info=user_info.to_dict(orient='records')                             # convert dataframe to list of dictionaries
      if len(user_info) == 0:
        raise ValueError('No user found for project {0}'.format(project_name))

      user_list=list()
      user_passwd_dict=dict()
      hpc_user=True                                                             # by default, load hpc user settings
      for user in user_info:
        username=user['username']                                               # get username for irods
        user_list.append(username)
        if 'ht_password' in user.keys():
          ht_passwd=user['ht_password']                                         # get htaccess passwd
          user_passwd_dict.update({username:ht_passwd})

        if 'category' in user.keys() and \
           'data_authority' in user.keys() and \
           user['category'] == 'NON_HPC_USER' and \
           user['data_authority']=='T':
          hpc_user=False                                                        # switch to non-hpc settings if primary user is non-hpc

      temp_work_dir=get_temp_dir()                                              # get a temp dir
      template_env=\
          Environment(loader=FileSystemLoader(\
                                searchpath=htaccess_template_path),
                      autoescape=select_autoescape(['html', 'xml']))            # set template env

      htaccess=template_env.get_template(htaccess_template)                     # read htaccess template
      htpasswd=template_env.get_template(htpasswd_template)                     # read htpass template
      htaccess_output=os.path.join(temp_work_dir,
                                   htaccess_filename)
      htpasswd_output=os.path.join(temp_work_dir,
                                   htpasswd_filename)

      htaccess.\
      stream(remote_project_dir=remote_project_path,
             project_tag=project_name,
             hpcUser=hpc_user,
             htpasswd_filename=htpasswd_filename,
             customerUsernameList=' '.join(user_list)).\
      dump(htaccess_output)                                                     # write new htacces file
      os.chmod(htaccess_output,
               mode=0o774)

      htpasswd.\
      stream(userDict=user_passwd_dict).\
      dump(htpasswd_output)                                                     # write new htpass file
      os.chmod(htpasswd_output,
               mode=0o774)

      template_prj=\
          Environment(loader=FileSystemLoader(\
                                searchpath=os.path.dirname(project_template_path)),
                      autoescape=select_autoescape(['txt', 'xml']))             # set template env for project
      project_index=\
          template_prj.\
          get_template(os.path.basename(project_template_path))                 # read htaccess template
      project_output=\
          os.path.join(temp_work_dir,
                       os.path.basename(project_template_path))
      project_index.\
      stream(ProjectName=project_name,
             seqrunInfoFile=seqruninfofile,
             sampleReadCountFile=samplereadcountfile,
             sampleReadCountCsvFile=samplereadcountcsvfile,
             ImageHeight=image_height).\
      dump(project_output)                                                      # write new project file
      os.chmod(project_output,
               mode=0o774)

      template_status=\
          Environment(loader=FileSystemLoader(\
                                searchpath=os.path.dirname(status_template_path)),
                      autoescape=select_autoescape(['txt', 'xml']))             # set template env for project
      project_status=\
          template_status.\
          get_template(os.path.basename(status_template_path))                  # read status page template
      status_output=\
          os.path.join(temp_work_dir,
                       os.path.basename(status_template_path))
      project_status.\
      stream(ProjectName=project_name,
             status_data_json=status_data_json).\
      dump(status_output)                                                       # write new project status file
      os.chmod(status_output,
               mode=0o774)

      template_analysis=\
          Environment(loader=FileSystemLoader(\
                                searchpath=os.path.dirname(analysis_template_path)),
                      autoescape=select_autoescape(['txt', 'xml']))             # set template env for analysis
      project_analysis=\
          template_analysis.\
          get_template(os.path.basename(analysis_template_path))                # read analysis page template
      analysis_output=\
          os.path.join(temp_work_dir,
                       os.path.basename(analysis_template_path))
      project_analysis.\
      stream(ProjectName=project_name,
             analysisInfoFile=analysis_data_json,
             analysisInfoCsvFile=analysis_data_csv
             ).\
      dump(analysis_output)                                                     # write new project analysis file
      os.chmod(analysis_output,
               mode=0o774)

      template_analysis_viewer=\
            Environment(loader=FileSystemLoader(\
                                  searchpath=os.path.dirname(analysis_viewer_template)),
                        autoescape=select_autoescape(['txt', 'xml']))           # set template env for analysis viewer
      project_analysis_viewer=\
          template_analysis_viewer.\
          get_template(os.path.basename(analysis_viewer_template))              # read analysis viewer page template
      analysis_viewer_output=\
          os.path.join(temp_work_dir,
                       os.path.basename(analysis_viewer_template))
      project_analysis_viewer.\
      stream(ProjectName=project_name,
             analysisJsFile=analysis_view_js).\
      dump(analysis_viewer_output)                                              # write new project analysis viewer file
      os.chmod(analysis_viewer_output,
               mode=0o774)

      remote_project_dir=\
            os.path.join(remote_project_path,
                         project_name)                                          # ger remote project dir path
      remote_htaccess_file=\
              os.path.join(remote_project_dir,
                           htaccess_filename)                                   # remote htaccess filepath
      self._check_and_copy_remote_file(\
              remote_user=remote_user,
              remote_host=remote_host,
              source_file=htaccess_output,
              remote_file=remote_htaccess_file)                                 # copy htaccess file to remote dir
      remote_htpasswd_file=\
              os.path.join(remote_project_dir,
                           htpasswd_filename)                                   # remote htpasswd filepath
      self._check_and_copy_remote_file(\
              remote_user=remote_user,
              remote_host=remote_host,
              source_file=htpasswd_output,
              remote_file=remote_htpasswd_file)                                 # copy htpasswd file to remote dir
      remote_project_output_file=\
              os.path.join(remote_project_dir,
                           os.path.basename(project_output))                    # remote project output filepath
      self._check_and_copy_remote_file(\
              remote_user=remote_user,
              remote_host=remote_host,
              source_file=project_output,
              remote_file=remote_project_output_file)                           # copy project output file to remote dir
      remote_status_output_file=\
              os.path.join(remote_project_dir,
                           os.path.basename(status_output))                     # remote project status output filepath
      self._check_and_copy_remote_file(\
              remote_user=remote_user,
              remote_host=remote_host,
              source_file=status_output,
              remote_file=remote_status_output_file)                            # copy project status output file to remote dir
      remote_analysis_output_file=\
              os.path.join(remote_project_dir,
                           os.path.basename(analysis_output))                   # remote project analysis output filepath
      self._check_and_copy_remote_file(\
              remote_user=remote_user,
              remote_host=remote_host,
              source_file=analysis_output,
              remote_file=remote_analysis_output_file)                          # copy project analysis output file to remote dir

      remote_analysis_viewer_output_file=\
              os.path.join(remote_project_dir,
                           os.path.basename(analysis_viewer_output))            # remote project analysis viewer output filepath
      self._check_and_copy_remote_file(\
              remote_user=remote_user,
              remote_host=remote_host,
              source_file=analysis_viewer_output,
              remote_file=remote_analysis_viewer_output_file)                   # copy project analysis viewer output file to remote dir
      self.param('dataflow_params',{'remote_dir_status':'done'})
      remove_dir(temp_work_dir)
    except Exception as e:
      message='seqrun: {2}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise

  @staticmethod
  def _calculate_image_height(sample_count,height=700,threshold=75):
    '''
    An internal static method for calculating image height based on the number
    of samples registered for any projects
    
    :param sample_count: Sample count for a given project
    :param height: Height of the image of display page, default 700
    :param threshold: Sample count threshold, default 75
    :returns: Revised image height
    '''
    try:
      if sample_count <= threshold:                                             # low sample count
        return height
      else:
        if (sample_count / threshold) <= 2:                                     # high sample count
          return height * 2
        else:                                                                   # very high sample count
          return int(height * (2+math.log(sample_count / threshold)))
    except:
      raise

  @staticmethod
  def _check_and_copy_remote_file(remote_user,remote_host,
                                  source_file,remote_file):
    '''
    An internal static method for copying files to remote path
    
    :param remote_user: Username for the remote server
    :param remote_host: Hostname for the remote server
    :param source_file: Source filepath
    :param remote_file: Remote filepath
    '''
    try:
      if not os.path.exists(source_file):
        raise IOError('Source file {0} not found for copy'.\
                      format(source_file))

      #check_remote_cmd=['ssh',
      #                  '{0}@{1}'.\
      #                  format(remote_user,
      #                         remote_host),
      #                  'ls',
      #                  '-a',
      #                  remote_file]                                            # remote check cmd
      #response=subprocess.call(check_remote_cmd)                                # look for existing remote file
      #if response !=0:
      #  rm_remote_cmd=['ssh',
      #                 '{0}@{1}'.\
      #                 format(remote_user,
      #                        remote_host),
      #                 'rm',
      #                 '-f',
      #                 remote_file]                                             # remote rm cmd
      #  subprocess.check_call(rm_remote_cmd)                                    # remove existing file

      copy_remote_file(\
        source_path=source_file,
        destinationa_path=remote_file,
        destination_address='{0}@{1}'.\
                            format(remote_user,
                                   remote_host),
        force_update=True)                                                      # create dir and copy file to remote
    except:
      raise
