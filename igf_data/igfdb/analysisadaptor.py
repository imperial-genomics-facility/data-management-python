import json
import pandas as pd
from sqlalchemy.sql import column
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.igfTables import Project, Analysis

class AnalysisAdaptor(BaseAdaptor):
  '''
  An adaptor class for Analysis table
  '''
  def store_analysis_data(self,data,autosave=True):
    '''
    A methodd for storing analysis data

    :param data: A dictionary or a dataframe. It sould have following columns

      * project_igf_id / project_id
      * analysis_name: A string
      * analysis_type: A string
      * analysis_description: A json encoded string

    :param autosave: A toggle for autocommit, default True
    :returns: None
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data=pd.DataFrame(data)
      if 'project_igf_id' in data.columns:
        project_map_function = \
          lambda x: \
            self.map_foreign_table_and_store_attribute(
              data=x,
              lookup_table=Project,
              lookup_column_name='project_igf_id',
              target_column_name='project_id')                                  # prepare the function for project id
        data['project_id'] = ''
        data = \
          data.apply(
            project_map_function,
            axis=1,
            result_type=None)                                                   # map project id foreign key id
        data.drop(
          'project_igf_id',
          axis=1,
          inplace=True)
      self.store_records(
        table=Analysis,
        data=data)
      if autosave:
        self.commit_session()
    except Exception as e:
      if autosave:
        self.rollback_session()
      raise ValueError(
        f'Failed to store analysis data, error: {e}')


  def fetch_project_igf_id_for_analysis_id(self,analysis_id):
    try:
      session = self.session
      query = \
        session.\
          query(Project.project_igf_id).\
          join(Analysis,Project.project_id==Analysis.project_id).\
          filter(Analysis.analysis_id==analysis_id)
      result = \
        self.fetch_records(
          query=query,
          output_mode='one_or_none')
      if result is None or \
         result.project_igf_id is None:
        raise ValueError(
                'No project id found for analysis {0}'.\
                  format(analysis_id))
      return result.project_igf_id
    except Exception as e:
      raise ValueError(
        f'Failed to fetch project id for analysis {analysis_id},error: {e}')


  def fetch_analysis_records_analysis_id(
        self,analysis_id,output_mode='dataframe'):
    '''
    A method for fetching analysis records using the analysis_id

    :param analysis_id: Analysis id form db
    :param :param output_mode:  dataframe / object, default: dataframe
    :returns: Analysis record
    '''
    try:
      session = self.session
      query = \
        session.\
        query(
          Analysis.analysis_id,
          Analysis.analysis_name,
          Analysis.analysis_type,
          Analysis.analysis_description).\
        filter(Analysis.analysis_id==analysis_id)
      results = \
        self.fetch_records(
          query=query,
          output_mode=output_mode)
      return results
    except Exception as e:
      raise ValueError(
        f'Failed to fetch analysis record, error: {e}')


  def fetch_analysis_records_project_igf_id(
        self,project_igf_id,output_mode='dataframe'):
    '''
    A method for fetching analysis records based on project_igf_id

    :param project_igf_id: A project_igf_id
    :param output_mode:  dataframe / object, default: dataframe
    :returns: Analysis record
    '''
    try:
      session = self.session
      query = \
        session.\
          query(Project.project_igf_id,
                Analysis.analysis_name,
                Analysis.analysis_type,
                Analysis.analysis_description).\
          join(Analysis,Project.project_id==Analysis.project_id).\
          filter(Project.project_igf_id==project_igf_id)
      results = \
        self.fetch_records(
          query=query,
          output_mode=output_mode)
      return results
    except Exception as e:
      raise ValueError(
        f'Failed to fetch analysis record, error: {e}')


  def fetch_analysis_record_by_analysis_name_and_project_igf_id(
        self, analysis_name, project_igf_id, output_mode='dataframe'):
    try:
      session = self.session
      query = \
        session.\
          query(
            Analysis.analysis_id,
            Analysis.analysis_name,
            Analysis.analysis_type,
            Project.project_igf_id,
            Analysis.analysis_description).\
          join(Analysis,Project.project_id==Analysis.project_id).\
          filter(Project.project_igf_id==project_igf_id).\
          filter(Analysis.analysis_name==analysis_name)
      results = \
        self.fetch_records(
          query=query,
          output_mode=output_mode)
      return results
    except Exception as e:
      raise ValueError(
        f"Failed to fetch analysis for {analysis_name} and {project_igf_id}, error: {e}")


  def check_analysis_record_by_analysis_name_and_project_id(
        self, analysis_name, project_id, output_mode='one_or_none'):
    try:
      session = self.session
      query = \
        session.\
          query(
            Analysis.analysis_id).\
          filter(Analysis.project_id==project_id).\
          filter(Analysis.analysis_name==analysis_name)
      results = \
        self.fetch_records(
          query=query,
          output_mode=output_mode)
      return results
    except Exception as e:
      raise ValueError(
        f"Failed to fetch analysis for {analysis_name} and {project_id}, error: {e}")