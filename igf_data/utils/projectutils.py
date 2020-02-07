import os,time,subprocess
import pandas as pd
from sqlalchemy import distinct
from datetime import datetime,timedelta
from dateutil.parser import parse
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from jinja2 import Template,Environment, FileSystemLoader,select_autoescape
from igf_data.utils.fileutils import check_file_path,get_temp_dir,remove_dir,copy_local_file
from igf_data.igfdb.igfTables import Base,Project,User,ProjectUser,Sample,Experiment,Run,Collection,Collection_group,File,Seqrun,Run_attribute,Project_attribute

def get_project_read_count(project_igf_id,session_class,run_attribute_name='R1_READ_COUNT',
                           active_status='ACTIVE'):
  '''
  A utility method for fetching sample read counts for an input project_igf_id
  
  :param project_igf_id: A project_igf_id string
  :param session_class: A db session class object
  :param run_attribute_name: Attribute name from Run_attribute table for read count lookup
  :param active_status: text label for active runs, default ACTIVE
  :returns: A pandas dataframe containing following columns
  
               * project_igf_id
               * sample_igf_id
               * flowcell_id
               * attribute_value
  '''
  try:
    read_count = pd.DataFrame()
    pr = ProjectAdaptor(**{'session_class':session_class})
    pr.start_session()
    query = \
      pr.session.\
        query(
          Project.project_igf_id,
          Sample.sample_igf_id,
          Seqrun.flowcell_id,
          Run_attribute.attribute_value).\
        join(Sample,Project.project_id==Sample.project_id).\
        join(Experiment,Sample.sample_id==Experiment.sample_id).\
        join(Run,Experiment.experiment_id==Run.experiment_id).\
        join(Seqrun,Seqrun.seqrun_id==Run.seqrun_id).\
        join(Run_attribute,Run.run_id==Run_attribute.run_id).\
        filter(Project.project_igf_id==project_igf_id).\
        filter(Sample.project_id==Project.project_id).\
        filter(Experiment.sample_id==Sample.sample_id).\
        filter(Run.experiment_id==Experiment.experiment_id).\
        filter(Seqrun.seqrun_id==Run.seqrun_id).\
        filter(Run_attribute.run_id==Run.run_id).\
        filter(Run_attribute.attribute_name==run_attribute_name).\
        filter(Run.status==active_status).\
        filter(Experiment.status==active_status).\
        filter(Sample.status==active_status)
    results = pr.fetch_records(query=query)
    pr.close_session()
    if len(results.index)>0:
      read_count = results
    return read_count
  except Exception as e:
    raise ValueError(
            "Failed to get project read count for {0}, error: {1}".\
              format(project_igf_id,e))


def get_seqrun_info_for_project(project_igf_id,session_class):
  '''
  A utility method for fetching seqrun_igf_id and flowcell_id which are linked
  to a specific project_igf_id

  :param project_igf_id: A project_igf_id string
  :param session_class: A db session class object
  :returns: A pandas dataframe containing following columns

    * seqrun_igf_id
    * flowcell_id
  '''
  try:
    seqrun_info = pd.DataFrame()
    pr = ProjectAdaptor(**{'session_class':session_class})
    pr.start_session()
    query = \
      pr.session.\
        query(
          distinct(Seqrun.seqrun_igf_id).\
            label('seqrun_igf_id'),
          Seqrun.flowcell_id).\
        join(Run,Seqrun.seqrun_id==Run.seqrun_id).\
        join(Experiment,Experiment.experiment_id==Run.experiment_id).\
        join(Sample,Sample.sample_id==Experiment.sample_id).\
        join(Project,Project.project_id==Sample.project_id).\
        filter(Project.project_id==Sample.project_id).\
        filter(Sample.sample_id==Experiment.sample_id).\
        filter(Experiment.experiment_id==Run.experiment_id).\
        filter(Run.seqrun_id==Seqrun.seqrun_id).\
        filter(Project.project_igf_id==project_igf_id)
    results = pr.fetch_records(query=query)
    pr.close_session()
    if len(results.index)>0:
      seqrun_info=results
    return seqrun_info
  except Exception as e:
    raise ValueError(
            "Failed to get seqrun info for the project {0}, error: {1}".\
              format(project_igf_id,e))


def mark_project_barcode_check_off(project_igf_id,session_class,
                                   barcode_check_attribute='barcode_check',
                                   barcode_check_val='OFF'):
  '''
  A utility method for marking project barcode check as off using the project_igf_id
  
  :param project_igf_id: A project_igf_id string
  :param session_class: A db session class object
  :param barcode_check_attribute: A text keyword for barcode check attribute, default barcode_check
  :param barcode_check_val: A text for barcode check attribute value, default is 'OFF'
  :returns: None
  '''
  try:
    db_connected = False
    pr = ProjectAdaptor(**{'session_class':session_class})
    pr.start_session()
    db_connected = True
    pr_attributes = \
      pr.check_project_attributes(
        project_igf_id=project_igf_id,
        attribute_name=barcode_check_attribute)                                 # check for the existing project attribute
    if pr_attributes:                                                           # if attribute present, then modify it
      project = \
        pr.fetch_project_records_igf_id(
          project_igf_id=project_igf_id)                                        # fetch project info
      
      pr.session.\
        query(Project_attribute).\
        filter(Project_attribute.attribute_name==barcode_check_attribute).\
        filter(Project_attribute.project_id==project.project_id).\
        update({Project_attribute.attribute_value:barcode_check_val,},
                synchronize_session=False)                                      # create query for fetching attribute records and modify attribute records
    else:                                                                       # if project attribute is not present, store it
      data = [{
        'project_igf_id':project_igf_id,
        'attribute_name':barcode_check_attribute,
        'attribute_value':barcode_check_val}]                                   # create data structure for the attribute table
      pr.store_project_attributes(data,autosave=False)                          # store data to attribute table without auto commit

    pr.commit_session()
  except Exception as e:
    if db_connected:
      pr.rollback_session()
    raise ValueError(
            "Failed to mark project barcode check off for {0}, error: {1}".\
              format(project_igf_id,e))
  finally:
    if db_connected:
      pr.close_session()


def get_files_and_irods_path_for_project(project_igf_id,db_session_class,irods_path_prefix='/igfZone/home/'):
  '''
  A function for listing all the files and irods dir path for a given project

  :param project_igf_id: A string containing the project igf id
  :param db_session_class: A database session object
  :param irods_path_prefix: A string containing irods path prefix, default '/igfZone/home/'
  :returns: A list containing all the files for a project and a string containing the irods path for the project
  '''
  try:
    file_list = list()
    irods_path = None
    base = ProjectAdaptor(**{'session_class':db_session_class})
    base.start_session()
    query_file_run = \
      base.session.\
        query(File.file_path).\
        join(Collection_group,File.file_id==Collection_group.file_id).\
        join(Collection,Collection.collection_id==Collection_group.collection_id).\
        join(Run,Run.run_igf_id==Collection.name).\
        join(Experiment,Experiment.experiment_id==Run.experiment_id).\
        join(Sample,Sample.sample_id==Experiment.sample_id).\
        join(Project,Project.project_id==Sample.project_id).\
        filter(Collection.table=='run').\
        filter(Project.project_igf_id==project_igf_id)
    run_files =  base.fetch_records(query_file_run)
    if len(run_files.index) > 0:
      run_files = list(run_files.get('file_path').values)
      file_list.\
        extend(run_files)
      query_file_exp = \
        base.session.\
          query(File.file_path).\
          join(Collection_group,File.file_id==Collection_group.file_id).\
          join(Collection,Collection.collection_id==Collection_group.collection_id).\
          join(Experiment,Experiment.experiment_igf_id==Collection.name).\
          join(Sample,Sample.sample_id==Experiment.sample_id).\
          join(Project,Project.project_id==Sample.project_id).\
          filter(Collection.table=='experiment').\
          filter(Project.project_igf_id==project_igf_id)
      exp_files = base.fetch_records(query_file_exp)
      user_query = \
        base.session.\
          query(User.username).\
          join(ProjectUser,User.user_id==ProjectUser.user_id).\
          join(Project,Project.project_id==ProjectUser.project_id).\
          filter(Project.project_igf_id==project_igf_id).\
          filter(ProjectUser.data_authority=='T')
      user_res = \
        base.fetch_records(user_query,output_mode='one_or_none')
      base.close_session()
      if len(exp_files.index) > 0:
        exp_files = list(exp_files.get('file_path').values)
        file_list.\
          extend(exp_files)
      if user_res.username is not None:
        irods_path = \
            os.path.join(irods_path_prefix,user_res.username,project_igf_id)
    return file_list,irods_path
  except Exception as e:
    raise ValueError(
            'Failed to fetch file_list and irods path, error: {0}'.\
              format(e))


def mark_project_as_withdrawn(project_igf_id,db_session_class,withdrawn_tag='WITHDRAWN'):
  '''
  A function for marking all the entries for a specific project as withdrawn

  :param project_igf_id: A string containing the project igf id
  :param db_session_class: A dbsession object
  :param withdrawn_tag: A string for withdrawn field in db, default WITHDRAWN
  :returns: None
  '''
  try:
    dbconnected = 0
    base = ProjectAdaptor(**{'session_class':db_session_class})
    base.start_session()
    dbconnected = 1
    base.session.\
      query(Project).\
      filter(Project.project_id==Sample.project_id).\
      filter(Sample.sample_id==Experiment.sample_id).\
      filter(Experiment.experiment_id==Run.experiment_id).\
      filter(Project.project_igf_id==project_igf_id).\
      update({
        Project.status:withdrawn_tag,
        Sample.status:withdrawn_tag,
        Experiment.status:withdrawn_tag,
        Run.status:withdrawn_tag},
        synchronize_session='fetch')                                            # marking project, sample, experiment, run
    base.session.\
      query(File).\
      filter(File.file_id==Collection_group.file_id).\
      filter(Collection.collection_id==Collection_group.collection_id).\
      filter(Run.run_igf_id==Collection.name).\
      filter(Experiment.experiment_id==Run.experiment_id).\
      filter(Sample.sample_id==Experiment.sample_id).\
      filter(Project.project_id==Sample.project_id).\
      filter(Collection.table=='run').\
      filter(Project.project_igf_id==project_igf_id).\
      update({File.status:withdrawn_tag},
              synchronize_session='fetch')                                      # marking run files
    base.session.\
      query(File).\
      filter(File.file_id==Collection_group.file_id).\
      filter(Collection.collection_id==Collection_group.collection_id).\
      filter(Experiment.experiment_igf_id==Collection.name).\
      filter(Sample.sample_id==Experiment.sample_id).\
      filter(Project.project_id==Sample.project_id).\
      filter(Collection.table=='experiment').\
      filter(Project.project_igf_id==project_igf_id).\
      update({File.status:withdrawn_tag},
              synchronize_session='fetch')                                    # marking exp files
    base.commit_session()
    base.close_session()
  except Exception as e:
    if dbconnected==1:
      base.rollback_session()
      base.close_session()
    raise ValueError(
            'Failed to mark project {0} as withdrawn, error: {1}'.\
              format(project_igf_id,e))


def mark_project_and_list_files_for_cleanup(project_igf_id,dbconfig_file,outout_dir,force_overwrite=True,
                                            use_ephemeral_space=False,irods_path_prefix='/igfZone/home/',
                                            withdrawn_tag='WITHDRAWN'):
  '''
  A wrapper function for project cleanup operation

  :param project_igf_id: A string of project igf -id
  :param dbconfig_file: A dbconf json file path
  :param outout_dir: Output dir path for dumping file lists for project
  :param force_overwrite: Overwrite existing output file, default True
  :param use_ephemeral_space: A toggle for temp dir, default False
  :param irods_path_prefix: Prefix for irods path, default /igfZone/home/
  :param withdrawn_tag: A string tag for marking files in db, default WITHDRAWN
  :returns: None
  '''
  try:
    check_file_path(dbconfig_file)
    check_file_path(outout_dir)
    dbparams = read_dbconf_json(dbconfig_file)
    pa = ProjectAdaptor(**dbparams)
    temp_dir = get_temp_dir(use_ephemeral_space=use_ephemeral_space)
    temp_project_file = os.path.join(temp_dir,'{0}_all_files.txt'.format(project_igf_id))
    final_project_file = os.path.join(outout_dir,'{0}_all_files.txt'.format(project_igf_id))
    temp_project_irods_file = os.path.join(temp_dir,'{0}_irods_files.txt'.format(project_igf_id))
    final_project_irods_file = os.path.join(outout_dir,'{0}_irods_files.txt'.format(project_igf_id))
    file_list, irods_path = \
      get_files_and_irods_path_for_project(
        project_igf_id=project_igf_id,
        db_session_class=pa.session_class,
        irods_path_prefix=irods_path_prefix)
    if len(file_list)==0:
      raise ValueError("No files found for project {0}".\
                         format(project_igf_id))
    if irods_path is None:
      raise ValueError("IRODs path not found for project {0}".\
                         format(project_igf_id))
    with open(temp_project_file,'w') as fp:
      fp.write('\n'.join(file_list))
    with open(temp_project_irods_file,'w') as fp:
      fp.write(irods_path)
    copy_local_file(
      temp_project_file,
      final_project_file,
      force=force_overwrite)
    copy_local_file(
      temp_project_irods_file,
      final_project_irods_file,
      force=force_overwrite)
    remove_dir(temp_dir)
    mark_project_as_withdrawn(
      project_igf_id=project_igf_id,
      db_session_class=pa.session_class,
      withdrawn_tag=withdrawn_tag)
  except Exception as e:
    raise ValueError(
            "Failed to mark project {0} for cleanup,error: {1}".\
              format(project_igf_id,e))


def find_projects_for_cleanup(dbconfig_file,warning_note_weeks=24,all_warning_note=False):
  '''
  A function for finding old projects for cleanup

  :param dbconfig_file: A dbconfig file path
  :param warning_note_weeks: Number of weeks from last sequencing run to wait before sending warnings, default 24
  :param all_warning_note: A toggle for sending warning notes to all, default False
  :returns: A list containing warning lists, a list containing final note list and another list with clean up list
  '''
  try:
    final_note_weeks = 5
    cleanup_note_week = 2
    warning_note_list = list()
    final_note_list = list()
    cleanup_list = list() 
    check_file_path(dbconfig_file)
    dbparam = read_dbconf_json(dbconfig_file)
    base = ProjectAdaptor(**dbparam)
    base.start_session()
    query = \
      base.session.\
        query(
          Project.project_igf_id,
          User.name,
          User.email_id,
          Sample.sample_igf_id,
          Experiment.experiment_igf_id,
          Run.run_igf_id,
          Run.lane_number,
          Seqrun.seqrun_igf_id,
          Seqrun.date_created).\
        join(ProjectUser,Project.project_id==ProjectUser.project_id).\
        join(User,User.user_id==ProjectUser.user_id).\
        join(Sample,Project.project_id==Sample.project_id).\
        join(Experiment,Sample.sample_id==Experiment.sample_id).\
        join(Run,Experiment.experiment_id==Run.experiment_id).\
        join(Seqrun,Run.seqrun_id==Seqrun.seqrun_id).\
        filter(Project.status=='ACTIVE').\
        filter(Seqrun.reject_run=='N').\
        filter(ProjectUser.data_authority=='T')
    result = base.fetch_records(query=query)
    base.close_session()
    warning_note_delta = timedelta(weeks=warning_note_weeks)
    final_note_delta =  warning_note_delta + timedelta(weeks=final_note_weeks)
    cleanup_note_delta = final_note_delta + timedelta(weeks=cleanup_note_week)
    for user_email_id,u_data in result.groupby('email_id'):
      user_name = u_data['name'].values[0]
      project_warn_list = list()
      project_final_list = list()
      project_cleanup_list = list()
      for project,p_data in u_data.groupby('project_igf_id'):
        last_run_date = \
          p_data.date_created.drop_duplicates().sort_values().values[0]
        last_run_date = parse(str(last_run_date))
        last_run_delta  = datetime.today() - last_run_date
        if (last_run_delta >= warning_note_delta) and \
           (last_run_delta < final_note_delta):
          project_warn_list.append(project)                                     # warn users about project clean up after one month
        elif (last_run_delta >= warning_note_delta) and \
             (last_run_delta < cleanup_note_delta):
          project_final_list.append(project)                                    # notify about cleanup operation
        elif last_run_delta >= cleanup_note_delta:
          project_cleanup_list.append(project)                                  # silent list

      if len(project_warn_list) > 0:
        cleanup_date = datetime.now() + timedelta(weeks=final_note_weeks)
        cleanup_date = cleanup_date.strftime('%d-%b-%Y')
        warning_note_list.\
          append({
            'email_id':user_email_id,
            'name':user_name,
            'cleanup_date':cleanup_date,
            'projects':project_warn_list})
      if len(project_final_list) > 0:
        cleanup_date = datetime.now()
        cleanup_date = cleanup_date.strftime('%d-%b-%Y')
        final_note_list.\
          append({
          'email_id':user_email_id,
          'name':user_name,
          'cleanup_date':cleanup_date,
          'projects':project_final_list})
      if len(project_cleanup_list) > 0:
        cleanup_date = datetime.now()
        cleanup_date = cleanup_date.strftime('%d-%b-%Y')
        cleanup_list.\
          append({
            'email_id':user_email_id,
            'name':user_name,
            'cleanup_date':cleanup_date,
            'projects':project_cleanup_list})
      if all_warning_note:
        warning_note_list.\
          extend(final_note_list)
        final_note_list = list()
        warning_note_list.\
          extend(cleanup_list)
        cleanup_list = list()
    return warning_note_list,final_note_list,cleanup_list
  except Exception as e:
    raise ValueError(
            "Failed to get list of projects for cleanup, error: {0}".\
              format(e))


def notify_project_for_cleanup(warning_template,final_notice_template,cleanup_template,
                               warning_note_list, final_note_list, cleanup_list,
                               use_ephemeral_space=False):
  '''
  A function for sending emails to users for project cleanup

  :param warning_template: A email template file for warning
  :param final_notice_template: A email template for final notice
  :param cleanup_template: A email template for sending cleanup list to igf
  :param warning_note_list: A list of dictionary containing following fields to warn user about cleanup

                 * name
                 * email_id
                 * projects
                 * cleanup_date

  :param final_note_list: A list of dictionary containing above mentioned fields to noftify user about final cleanup
  :param cleanup_list: A list of dictionary containing above mentioned fields to list projects for cleanup
  :param use_ephemeral_space: A toggle for using the ephemeral space, default False
  '''
  try:
    check_file_path(warning_template)
    check_file_path(final_notice_template)
    check_file_path(cleanup_template)
    if not isinstance(warning_note_list,list) or \
       not isinstance(final_note_list,list) or \
       not isinstance(cleanup_list,list):
      raise TypeError(
              "Expection a list, got: {0}, {1}, {2}".
                format(
                  type(warning_note_list),
                  type(final_note_list),
                  type(cleanup_list)))
    temp_dir = get_temp_dir(use_ephemeral_space=use_ephemeral_space)
    warning_email_counter = 0
    for data in warning_note_list:
      warning_email_counter += 1
      temp_draft = \
        os.path.join(
          temp_dir,
          'warning_email_{0}.txt'.\
            format(warning_email_counter))
      draft_email_for_project_cleanup(
        template_file=warning_template,
        data=data,
        draft_output=temp_draft)
      send_email_to_user_via_sendmail(
        draft_email_file=temp_draft)
    final_notice_email_counter = 0
    for data in final_note_list:
      final_notice_email_counter += 1
      temp_draft = \
        os.path.join(
          temp_dir,
          'final_notice_email_{0}.txt'.\
            format(final_notice_email_counter))
      draft_email_for_project_cleanup(
        template_file=final_notice_template,
        data=data,
        draft_output=temp_draft)
      send_email_to_user_via_sendmail(
        draft_email_file=temp_draft)
    if len(cleanup_list) >0:
      temp_draft = \
        os.path.join(
          temp_dir,
          'cleanup_email_0.txt')
      draft_email_for_project_cleanup(
        template_file=cleanup_template,
        data=cleanup_list,
        draft_output=temp_draft)
      send_email_to_user_via_sendmail(
        draft_email_file=temp_draft)
    remove_dir(temp_dir)
  except Exception as e:
    raise ValueError("Failed to run clean up scripts, error: {0}".format(e))


def draft_email_for_project_cleanup(template_file,data,draft_output):
  '''
  A method for drafting email for cleanup

  :param template_file: A template file
  :param data: A list of dictionary or a dictionary containing the following columns

                * name
                * email_id
                * projects
                * cleanup_date

  :param draft_output: A output filename
  '''
  try:
    template_env = \
        Environment(\
          loader=\
            FileSystemLoader(
              searchpath=os.path.dirname(template_file)),
          autoescape=select_autoescape(['html','xml']))
    template_file = \
      template_env.\
        get_template(os.path.basename(template_file))
    
    if isinstance(data,dict):
      name = data.get('name')
      email_id = data.get('email_id')
      projects = data.get('projects')
      cleanup_date = data.get('cleanup_date')
      if (name is None) or (email_id is None) or \
         (projects is None) or (cleanup_date is None):
        raise ValueError(
                "Failed to get required info in data: {0}".\
                  format(data))
      template_file.\
        stream(\
          customerEmail=email_id,
          customerName=name,
          projectList=projects,
          projectDeadline=cleanup_date).\
        dump(draft_output)
    elif isinstance(data,dict):
      template_file.\
        stream(\
          projectInfoList=data).\
        dump(draft_output)
  except Exception as e:
    raise ValueError(
            "Failed to draft email for project cleanup, error: {0}".\
              format(e))


def send_email_to_user_via_sendmail(draft_email_file,waiting_time=20,sendmail_exe='sendmail',dry_run=False):
  '''
  A function for sending email to users via sendmail

  :param draft_email_file: A draft email to be sent to user
  :param waiting_time: Wait after sending the email, default 20sec
  :param sendmail_exe: Sendmail exe path, default sendmail
  :param dry_run: A toggle for dry run, default False
  '''
  try:
    check_file_path(draft_email_file)
    proc = \
      subprocess.\
        Popen([
          'cat',draft_email_file],
          stdout=subprocess.PIPE)
    sendmail_cmd = [
      sendmail_exe,'-t']
    if dry_run:
      return sendmail_cmd
    subprocess.\
      check_call(
        sendmail_cmd,
        stdin=proc.stdout)
    proc.stdout.close()
    time.sleep(waiting_time)                                                    # wait for 20 secs
  except Exception as e:
    raise ValueError(
            "Failed to send email {0}, error: {1}".\
              format(draft_email_file,e))