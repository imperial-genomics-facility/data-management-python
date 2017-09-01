import json
import pandas as pd
from sqlalchemy.sql import column
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.igfTables import Project, Analysis

class AnalysisAdaptor(BaseAdaptor):
  '''
  An adaptor class for Analysis table
  '''
  def store_analysis_data(self, data, autosave=True):
    '''
    A methodd for storing analysis data
    required params:
    data: A dictionary or a dataframe. It sould have following columns
          project_igf_id / project_id
          analysis_type: (RNA_DIFFERENTIAL_EXPRESSION/RNA_TIME_SERIES/CHIP_PEAK_CALL/SOMATIC_VARIANT_CALLING)
          analysis_description 
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data=pd.DataFrame(data)
      
      if 'project_igf_id' in data.columns:
        project_map_function=lambda x: self.map_foreign_table_and_store_attribute( \
                                                data=x, \
                                                lookup_table=Project, \
                                                lookup_column_name='project_igf_id', \
                                                target_column_name='project_id')       # prepare the function for project id
        new_data=data.apply(project_map_function, axis=1)                              # map project id foreign key id
        data=new_data

      self.store_records(table=Analysis, data=data)
      if autosave:
        self.commit_session()
    except:
      if autosave:
        self.rollback_session()
      raise


  def fetch_analysis_records_project_igf_id(self, project_igf_id='', output_mode='dataframe',):
    '''
    A method for fetching analysis records based on project_igf_id
    required params:
      project_igf_id: default: null
      output_mode:   dataframe / object, default: dataframe
    '''
    session=self.session
    query=session.query(Project.project_igf_id, Analysis.analysis_type, Analysis.analysis_description).join(Analysis)
    if project_igf_id:
      query=query.filter(Project.project_igf_id==project_igf_id)

    try:    
      results=self.fetch_records(query=query, output_mode=output_mode)
      return results
    except:
      raise



