import os
import pandas as pd
from sqlalchemy import distinct
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.igfTables import Project,Sample,Seqrun,Experiment,Run,Run_attribute, Project_attribute

def get_project_read_count(project_igf_id,session_class,run_attribute_name='R1_READ_COUNT',
                           active_status='ACTIVE'):
  '''
  A utility method for fetching sample read counts for an input project_igf_id
  
  :param project_igf_id: A project_igf_id string
  :param session_class: A db session class object
  :param run_attribute_name: Attribute name from Run_attribute table for read count lookup
  :param active_status: text label for active runs, default ACTIVE
  :returns: A pandas dataframe containing following columns
  
               project_igf_id
               sample_igf_id
               flowcell_id
               attribute_value
  '''
  try:
    read_count=pd.DataFrame()
    pr=ProjectAdaptor(**{'session_class':session_class})
    pr.start_session()
    query=pr.session.query(Project.project_igf_id,
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
    results=pr.fetch_records(query=query)
    pr.close_session()
    if len(results.index)>0:
      read_count=results
    return read_count
  except:
    raise

def get_seqrun_info_for_project(project_igf_id,session_class):
  '''
  A utility method for fetching seqrun_igf_id and flowcell_id which are linked
  to a specific project_igf_id
  
  required params:
  project_igf_id: A project_igf_id string
  session_class: A db session class object
  
  returns a pandas dataframe containing following columns
    seqrun_igf_id
    flowcell_id
  '''
  try:
    seqrun_info=pd.DataFrame()
    pr=ProjectAdaptor(**{'session_class':session_class})
    pr.start_session()
    query=pr.session.query(distinct(Seqrun.seqrun_igf_id).\
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
    results=pr.fetch_records(query=query)
    pr.close_session()
    if len(results.index)>0:
      seqrun_info=results
    return seqrun_info
  except:
    raise

def mark_project_barcode_check_off(project_igf_id,session_class,
                                   barcode_check_attribute='barcode_check',
                                   barcode_check_val='OFF'):
  '''
  A utility method for marking project barcode check as off using the project_igf_id
  
  :param project_igf_id: A project_igf_id string
  :param session_class: A db session class object
  :param barcode_check_attribute: A text keyword for barcode check attribute, default barcode_check
  :param barcode_check_val: A text for barcode check attribute value, default is 'OFF'
  '''
  try:
    db_connected=False
    pr=ProjectAdaptor(**{'session_class':session_class})
    pr.start_session()
    db_connected=True
    pr_attributes=pr.check_project_attributes(project_igf_id=project_igf_id,
                                              attribute_name=barcode_check_attribute) # check for the existing project attribute
    if pr_attributes:                                                           # if attribute present, then modify it
      project=pr.fetch_project_records_igf_id(project_igf_id=project_igf_id)    # fetch project info
      query=pr.session.\
            query(Project_attribute).\
            filter(Project_attribute.attribute_name==barcode_check_attribute).\
            filter(Project_attribute.project_id==project.project_id).\
            update({Project_attribute.attribute_value:barcode_check_val,},
                   synchronize_session=False)                                   # create query for fetching attribute records and modify attribute records
    else:                                                                       # if project attribute is not present, store it
      data=[{'project_igf_id':project_igf_id,
             'attribute_name':barcode_check_attribute,
             'attribute_value':barcode_check_val}]                              # create data structure for the attribute table
      pr.store_project_attributes(data,autosave=False)                          # store data to attribute table without auto commit

    pr.commit_session()
  except:
    if db_connected:
      pr.rollback_session()
    raise
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
    base = BaseAdaptor(**{'session_class':db_session_class})
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
    raise ValueError('Failed to fetch file_list and irods path, error: {0}'.format(e))