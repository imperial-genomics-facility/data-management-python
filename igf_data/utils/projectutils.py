from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.igfTables import Project,Sample,Seqrun,Experiment,Run,Run_attribute

def get_project_read_count(project_igf_id,session_class,run_attribute_name='R1_READ_COUNT'):
  '''
  '''
  try:
    read_count=list()
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
    if (results.index)>0:
      read_count=results.to_dict(orient='region')
    return read_count
  except:
    raise