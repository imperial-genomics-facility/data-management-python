import pandas as pd
from sqlalchemy import distinct
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.igfTables import Project,Sample,Seqrun,Experiment,Run,Run_attribute

def get_project_read_count(project_igf_id,session_class,run_attribute_name='R1_READ_COUNT'):
  '''
  A utility method for fetching sample read counts for an input project_igf_id
  required params:
  project_igf_id: A project_igf_id string
  session_class: A db session class object
  run_attribute_name: Attribute name from Run_attribute table for read count lookup
  
  returns a pandas dataframe containing following columns
    project_igf_id
    sample_igf_id
    seqrun-igf_id
    attribute_value
  '''
  try:
    read_count=pd.DataFrame()
    pr=ProjectAdaptor(**{'session_class':session_class})
    pr.start_session()
    query=pr.session.query(Project.project_igf_id,
                           Sample.sample_igf_id,
                           Seqrun.seqrun_igf_id,
                           Run_attribute.attribute_value).\
                     join(Sample).\
                     join(Experiment).\
                     join(Run).\
                     join(Seqrun).\
                     join(Run_attribute).\
                     filter(Project.project_igf_id==project_igf_id).\
                     filter(Sample.project_id==Project.project_id).\
                     filter(Experiment.sample_id==Sample.sample_id).\
                     filter(Run.experiment_id==Experiment.experiment_id).\
                     filter(Seqrun.seqrun_id==Run.seqrun_id).\
                     filter(Run_attribute.run_id==Run.run_id).\
                     filter(Run_attribute.attribute_name==run_attribute_name)
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
    query=pr.session.query(distinct(Seqrun.seqrun_igf_id),
                           Seqrun.flowcell_id).\
                     join(Run).\
                     join(Experiment).\
                     join(Sample).\
                     join(Project).\
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