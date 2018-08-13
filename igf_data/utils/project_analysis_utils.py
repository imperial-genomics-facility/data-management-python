import os
import pandas as pd
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.igfTables import Base, Project,Sample,Experiment,Run,Seqrun,Collection,Collection_group,File
from igf_data.utils.project_data_display_utils import convert_project_data_gviz_data

class Project_analysis:
  '''
  A class for fetching all the analysis files linked to a project
  
  :param igf_session_class: A database session class
  :param collection_type_list: A list of collection type for database lookup
  :param remote_analysis_dir: A remote path prefix for analysis file look up, default analysis
  '''
  def __init__(self,igf_session_class,collection_type_list,remote_analysis_dir='analysis'):
    try:
      self.igf_session_class=igf_session_class
      if not isinstance(collection_type_list, list):
        raise ValueError('Expecting a list of collection types for db look up')

      self.collection_type_list=collection_type_list
      self.remote_analysis_dir=remote_analysis_dir
    except:
      raise

  def get_analysis_data_for_project(self,project_igf_id,output_file,gviz_out=True):
    '''
    A method for fetching all the analysis files for a project
    
    :param project_igf_id: A project igf id for database lookup
    :param output_file: An output filepath, either a csv or a gviz json
    :param gviz_out: A toggle for converting output to gviz output, default is True
    '''
    try:
      base=BaseAdaptor({'session_class':self.igf_session_class})
      base.start_session()                                                      # connect to database
      query=base.session.\
            query(Sample.sample_igf_id,File.file_path).\
            join(Project).\
            join(Experiment).\
            join(Collection).\
            join(Collection_group).\
            join(File).\
            filter(Project.project_igf_id==project_igf_id).\
            filter(Collection.table=='experiment').\
            filter(Collection.table.in_(self.collection_type_list))
      results=base.fetch_records(query=query,
                                 output_mode='dataframe')
      base.close_session()
    except:
      raise

if __name__=='__main__':
  from sqlalchemy import create_engine
  from igf_data.utils.dbutils import read_dbconf_json
  from igf_data.igfdb.fileadaptor import FileAdaptor
  from igf_data.igfdb.projectadaptor import ProjectAdaptor
  from igf_data.igfdb.sampleadaptor import SampleAdaptor
  from igf_data.igfdb.experimentadaptor import ExperimentAdaptor
  from igf_data.igfdb.collectionadaptor import CollectionAdaptor
  
  dbconfig = 'data/dbconfig.json'
  dbparam=read_dbconf_json(self.dbconfig)
  base = BaseAdaptor(**dbparam)
  base.start_session()
  project_data=[{'project_igf_id':'ProjectA'}]
  pa=ProjectAdaptor(**{'session':base.session})
  pa.store_project_and_attribute_data(data=project_data)                        # load project data
  sample_data=[{'sample_igf_id':'SampleA',
                'project_igf_id':'ProjectA'}]                                   # sample data
  sa=SampleAdaptor(**{'session':base.session})
  sa.store_sample_and_attribute_data(data=sample_data)                          # store sample data
  experiment_data=[{'experiment_igf_id':'ExperimentA',
                    'sample_igf_id':'SampleA',
                    'library_name':'SampleA',
                    'platform_name':'MISEQ',
                    'project_igf_id':'ProjectA'}]                               # experiment data
  ea=ExperimentAdaptor(**{'session':base.session})
  ea.store_project_and_attribute_data(data=experiment_data)
  
  base.close_session()