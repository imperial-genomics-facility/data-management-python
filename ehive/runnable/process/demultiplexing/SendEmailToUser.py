import os, subprocess
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from jinja2 import Template,Environment, FileSystemLoader,select_autoescape
from igf_data.utils.fileutils import get_temp_dir, remove_dir

class SendEmailToUser(IGFBaseProcess):
  '''
  A runnable module for sending email to the users about the data availability
  '''
  def param_defaults(self):
    params_dict=super(SendEmailToUser,self).param_defaults()
    params_dict.update({
        'email_template_path':'email_notification',
        'email_template':'send_fastq_to_user.txt',
        'remote_user':None,
        'remote_host':None,
        'user_info_file':None,
        'sendmail_exe':'/usr/sbin/sendmail',
      })
    return params_dict
  
  def run(self):
    try:
      seqrun_igf_id=self.param_required('seqrun_igf_id')
      project_name=self.param_required('project_name')
      seqrun_date=self.param_required('seqrun_date')
      flowcell_id=self.param_required('flowcell_id')
      igf_session_class=self.param_required('igf_session_class')
      template_dir=self.param_required('template_dir')
      remote_user=self.param_required('remote_user')
      remote_host=self.param_required('remote_host')
      user_info_file=self.param('user_info_file')
      email_template_path=self.param('email_template_path')
      email_template=self.param('email_template')
      sendmail_exe=self.param('sendmail_exe')
      hpcUser=False                                                             # default value for hpc users

      pa=ProjectAdaptor(**{'session_class':igf_session_class})
      pa.start_session()
      user_info=pa.get_project_user_info(project_igf_id=project_name)           # fetch user info from db
      pa.close_session()
      
      user_info=user_info[user_info['data_authority']=='T']                     # filter dataframe for data authority
      user_info=user_info.to_dict(orient='records')                             # convert dataframe to list of dictionaries
      if len(user_info) == 0:
        raise ValueError('No user found for project {0}'.format(project_name))
    
      user_info=user_info[0]
      user_name=user_info['name']                                               # get username for irods
      login_name=user_info['username']
      user_email=user_info['email_id']
      user_category=user_info['category']
      if user_category=='HPC_USER':
        hpcUser=True                                                            # set value for hpc user
        message='loading hpc user specific settings for {0}:{1}'.\
                format(user_name,login_name)
        self.post_message_to_slack(message,reaction='pass')                     # send message to slack

      email_template_path=os.path.join(template_dir, \
                                       email_template_path)
      template_env=Environment(loader=FileSystemLoader(searchpath=email_template_path), \
                               autoescape=select_autoescape(['html','xml']))    # set template env
      template_file=template_env.get_template(email_template)
      temp_work_dir=get_temp_dir()                                              # get a temp dir
      report_output_file=os.path.join(temp_work_dir,email_template)
      template_file.\
        stream(projectName=project_name, \
               customerEmail=user_email, \
               customerName=user_name, \
               customerUsername=login_name,\
               projectRunDate=seqrun_date, \
               flowcellId=flowcell_id, \
               hpcUser=hpcUser,\
              ).\
        dump(report_output_file)
      proc=subprocess.Popen(['cat',\
                             report_output_file
                            ], \
                            stdout=subprocess.PIPE)
      sendmail_cmd=[sendmail_exe,\
                    '-t',\
                   ]
      subprocess.check_call(sendmail_cmd,stdin=proc.stdout)
      proc.stdout.close()
      remove_dir(temp_work_dir)
      message='finished data processing for seqrun: {0}, project: {1}, sent mail to igf'.\
              format(seqrun_igf_id, project_name)
      self.post_message_to_slack(message,reaction='pass')
    except Exception as e:
      message='seqrun: {2}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise
    
    
  def _get_user_passwd(self,user_email):
    '''
    An internal method for fetching the user passwd
    It should be redundant now as password will not be sent along with the data
    '''
    try:
      remote_user=self.param_required('remote_user')
      remote_host=self.param_required('remote_host')
      user_info_file=self.param_required('user_info_file')
      
      cmd=['ssh',\
           '{0}@{1}'.format(remote_user, remote_host), \
           'grep', \
           '-w',\
           user_email,
           user_info_file
          ]
      proc1=subprocess.Popen(cmd,stdout=subprocess.PIPE)
      user_info=proc1.communicate()[0]
      user_info=user_info.decode('UTF-8')
      if user_info is '':
        raise ValueError('No information was found for user email {0}'.\
                         format(user_email))
      
      (name,user,passwd,email,date,info)=user_info.split(',')
      if email != user_email:
        raise ValueError('couldn\'t resolve email ids: {0}, {1}'.\
                         format(email,user_email))
      return passwd
    except:
      raise